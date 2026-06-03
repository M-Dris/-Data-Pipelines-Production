"""
DAG : catalog_ingestion_pipeline
=================================
Ingère le catalogue musical depuis les fichiers JSON des labels
(stockés dans MinIO) et les charge dans PostgreSQL.

Planification : quotidienne à 02:00 UTC
Catchup       : activé (permet le backfill historique)

Architecture :
    MinIO (labels/*.json)
        → extract_from_minio()
        → validate_schema()
        → transform_catalog()        ← normalisation, dédoublonnage
        → load_to_postgres()         ← upsert avec ON CONFLICT
        → notify_success()
"""

from datetime import datetime, timedelta
import logging
import os

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

# ─────────────────────────────────────────────────────────────
# DOCUMENTATION DU DAG (obligatoire pour la note)
# ─────────────────────────────────────────────────────────────

DAG_DOC = """
## catalog_ingestion_pipeline

### Rôle
Ingère les métadonnées musicales depuis les fichiers JSON de 3 labels
(SunSet Records, NightWave Music, Urban Pulse) stockés dans MinIO.

### Sources
- `s3://labels-raw/sunset_records.json`
- `s3://labels-raw/nightwave_music.json`
- `s3://labels-raw/urban_pulse.json`

### Destinations
- Table `artists` (upsert)
- Table `albums` (upsert)
- Table `tracks` (upsert)

### Idempotence
Le pipeline est idempotent : relancer plusieurs fois le même DAGrun
produit le même résultat grâce aux upserts ON CONFLICT DO UPDATE.

### Gestion des erreurs
- Schéma invalide → événement en DLQ (`dead_letter_events`)
- MinIO indisponible → retry x3 avec backoff exponentiel

### Monitoring
- XCom `tracks_inserted` : nombre de tracks insérées/mises à jour
- XCom `errors_count` : nombre d'entrées envoyées en DLQ
"""

# ─────────────────────────────────────────────────────────────
# CONFIGURATION PAR DÉFAUT
# ─────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner":                     "spotify-team",
    "depends_on_past":           False,
    "start_date":                datetime(2025, 1, 1),
    "email_on_failure":          False,
    "email_on_retry":            False,
    "retries":                   3,
    "retry_delay":               timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout":         timedelta(minutes=30),
}

POSTGRES_CONN_ID = "spotify_postgres"
MINIO_CONN_ID    = "spotify_minio"
MINIO_BUCKET     = "labels-raw"
LABEL_FILES      = ["sunset_records.json", "nightwave_music.json", "urban_pulse.json"]

# Champs obligatoires pour la validation
REQUIRED_ARTIST_FIELDS = ["id", "name", "label"]
REQUIRED_ALBUM_FIELDS  = ["id", "artist_id", "title"]
REQUIRED_TRACK_FIELDS  = ["id", "artist_id", "title", "duration_ms"]


# ─────────────────────────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="catalog_ingestion_pipeline",
    default_args=DEFAULT_ARGS,
    description="Ingestion quotidienne du catalogue musical depuis MinIO vers PostgreSQL",
    schedule_interval="0 2 * * *",
    catchup=True,
    max_active_runs=1,
    tags=["spotify", "phase-1", "ingestion", "catalogue"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="extract_from_minio")
    def extract_from_minio(**context) -> list[dict]:
        import boto3
        import json
        from botocore.exceptions import ClientError

        s3 = boto3.client(
            's3',
            endpoint_url='http://minio:9000',
            aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("MINIO_SECRET_KEY")
        )

        catalogs = []
        for filename in LABEL_FILES:
            local_path = f"/opt/airflow/src/data/labels/{filename}"
            
            # Upload vers MinIO
            with open(local_path, "rb") as f:
                s3.put_object(Bucket=MINIO_BUCKET, Key=filename, Body=f, ContentType="application/json")
                logging.info(f"⬆️ Fichier uploadé : {filename}")

            # Télécharge et retourne
            obj = s3.get_object(Bucket=MINIO_BUCKET, Key=filename)
            catalog = json.loads(obj['Body'].read())
            catalogs.append(catalog)
            logging.info(f"✅ Fichier téléchargé : {filename}")

        return catalogs

    @task(task_id="validate_schema")
    def validate_schema(raw_catalogs: list[dict]) -> dict:
        """
        Valide le schéma de chaque catalogue.
        - Vérifie les champs obligatoires pour artists, albums, tracks
        - Les entrées invalides partent en DLQ (dead_letter_events)
        - Retourne les données valides + le nombre d'erreurs
        """
        import json
        import uuid
        from datetime import timezone

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        valid_artists = []
        valid_albums  = []
        valid_tracks  = []
        errors_count  = 0

        for catalog in raw_catalogs:
            # Valider les artistes
            for artist in catalog.get("artists", []):
                if all(field in artist for field in REQUIRED_ARTIST_FIELDS):
                    valid_artists.append(artist)
                else:
                    errors_count += 1
                    cursor.execute("""
                        INSERT INTO dead_letter_events
                            (id, original_topic, error_type, payload, created_at)
                        VALUES (%s, %s, %s, %s, NOW())
                    """, (
                        str(uuid.uuid4()),
                        "catalog_ingestion",
                        "schema_validation",
                        json.dumps(artist)
                    ))
                    logging.warning(f"⚠️ Artiste invalide envoyé en DLQ : {artist}")

            # Valider les albums
            for album in catalog.get("albums", []):
                if all(field in album for field in REQUIRED_ALBUM_FIELDS):
                    valid_albums.append(album)
                else:
                    errors_count += 1
                    cursor.execute("""
                        INSERT INTO dead_letter_events
                            (id, original_topic, error_type, payload, created_at)
                        VALUES (%s, %s, %s, %s, NOW())
                    """, (
                        str(uuid.uuid4()),
                        "catalog_ingestion",
                        "schema_validation",
                        json.dumps(album)
                    ))
                    logging.warning(f"⚠️ Album invalide envoyé en DLQ : {album}")

            # Valider les tracks
            for track in catalog.get("tracks", []):
                if all(field in track for field in REQUIRED_TRACK_FIELDS):
                    valid_tracks.append(track)
                else:
                    errors_count += 1
                    cursor.execute("""
                        INSERT INTO dead_letter_events
                            (id, original_topic, error_type, payload, created_at)
                        VALUES (%s, %s, %s, %s, NOW())
                    """, (
                        str(uuid.uuid4()),
                        "catalog_ingestion",
                        "schema_validation",
                        json.dumps(track)
                    ))
                    logging.warning(f"⚠️ Track invalide envoyé en DLQ : {track}")

        conn.commit()
        cursor.close()
        conn.close()

        logging.info(f"✅ Validation terminée — {len(valid_tracks)} tracks valides, {errors_count} erreurs")

        return {
            "valid": {
                "artists": valid_artists,
                "albums":  valid_albums,
                "tracks":  valid_tracks,
            },
            "errors_count": errors_count
        }

    @task(task_id="transform_catalog")
    def transform_catalog(validated: dict) -> dict:
        """
        Normalise les données du catalogue.
        - Noms d'artistes : strip + title case
        - Durées de tracks : filtre les valeurs aberrantes
        - Supprime les doublons par id
        """
        data = validated["valid"]

        # Normaliser les artistes
        seen_artist_ids = set()
        clean_artists = []
        for artist in data["artists"]:
            if artist["id"] in seen_artist_ids:
                continue
            seen_artist_ids.add(artist["id"])
            artist["name"] = artist["name"].strip().title()
            clean_artists.append(artist)

        # Normaliser les albums
        seen_album_ids = set()
        clean_albums = []
        for album in data["albums"]:
            if album["id"] in seen_album_ids:
                continue
            seen_album_ids.add(album["id"])
            album["title"] = album["title"].strip().title()
            clean_albums.append(album)

        # Normaliser les tracks
        seen_track_ids = set()
        clean_tracks = []
        for track in data["tracks"]:
            if track["id"] in seen_track_ids:
                continue
            seen_track_ids.add(track["id"])
            # Filtrer les durées aberrantes (< 0 ou > 1 heure)
            if not (0 < track["duration_ms"] < 3_600_000):
                logging.warning(f"⚠️ Track avec durée invalide ignorée : {track['id']}")
                continue
            track["title"] = track["title"].strip().title()
            clean_tracks.append(track)

        logging.info(f"✅ Transformation : {len(clean_artists)} artistes, {len(clean_albums)} albums, {len(clean_tracks)} tracks")

        return {
            "artists": clean_artists,
            "albums":  clean_albums,
            "tracks":  clean_tracks,
        }

    @task(task_id="load_to_postgres")
    def load_to_postgres(transformed: dict, **context) -> dict:
        """
        Charge les données dans PostgreSQL avec upsert idempotent.
        - ON CONFLICT DO UPDATE pour artists, albums, tracks
        - Idempotent : relancer 10 fois = même résultat
        """
        import json

        hook   = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn   = hook.get_conn()
        cursor = conn.cursor()

        artist_tuples = [
            (
                a["id"],
                a["name"],
                a.get("label", ""),
                a.get("genres", []),
                a.get("monthly_listeners", 0),
            )
            for a in transformed["artists"]
        ]
        cursor.executemany("""
            INSERT INTO artists (id, name, label, genres, monthly_listeners)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (name, label) DO UPDATE SET
                monthly_listeners = EXCLUDED.monthly_listeners,
                updated_at        = NOW()
        """, artist_tuples)

        # ── Upsert Albums ─────────────────────────────────────
        album_tuples = [
            (
                a["id"],
                a["artist_id"],
                a["title"],
                a.get("release_year"),
                a.get("total_tracks", 0),
            )
            for a in transformed["albums"]
        ]
        cursor.executemany("""
            INSERT INTO albums (id, artist_id, title, release_year, total_tracks)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                title        = EXCLUDED.title
        """, album_tuples)

        # ── Upsert Tracks ─────────────────────────────────────
        track_tuples = [
            (
                t["id"],
                t["album_id"],
                t["artist_id"],
                t["title"],
                t["duration_ms"],
                t.get("genre", ""),
                t.get("bpm", 0),
                t.get("explicit", False),
                t.get("audio_file_path", ""),
            )
            for t in transformed["tracks"]
        ]
        cursor.executemany("""
            INSERT INTO tracks
                (id, album_id, artist_id, title, duration_ms, genre, bpm, explicit, audio_file_path)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                updated_at = NOW()
        """, track_tuples)

        conn.commit()
        cursor.close()
        conn.close()

        stats = {
            "artists_inserted": len(artist_tuples),
            "albums_inserted":  len(album_tuples),
            "tracks_inserted":  len(track_tuples),
            "errors_count":     0,
        }

        logging.info(f"✅ Chargement terminé : {stats}")
        return stats

    @task(task_id="notify_success")
    def notify_success(stats: dict, **context):
        """
        Log de succès avec statistiques d'ingestion.
        """
        dag_run = context["dag_run"]
        print(f"""
        ✅ catalog_ingestion_pipeline terminé
        DAGRun          : {dag_run.run_id}
        Tracks insérées : {stats.get('tracks_inserted', 0)}
        Artists insérés : {stats.get('artists_inserted', 0)}
        Albums insérés  : {stats.get('albums_inserted', 0)}
        Erreurs DLQ     : {stats.get('errors_count', 0)}
        """)

    # ── Orchestration des tâches ──────────────────────────────
    raw         = extract_from_minio()
    validated   = validate_schema(raw)
    transformed = transform_catalog(validated)
    stats       = load_to_postgres(transformed)
    notify_success(stats)


if __name__ == "__main__":
    dag.test()