"""
DAG : streaming_events_pipeline
=================================
Consomme les événements d'écoute depuis Redis (pub/sub),
les valide, les enrichit avec le catalogue et les stocke.

Planification : toutes les 5 minutes
Catchup       : désactivé (micro-batch temps réel)

Architecture :
    Redis (pub/sub listening_events + p2p_network_events)
        → consume_from_redis()
        → validate_events()          ← invalides → DLQ
        → enrich_events()            ← jointure catalogue PostgreSQL
        → store_to_parquet()         ← MinIO partitionné par heure
        → upsert_to_postgres()       ← table listening_events

TODO :
    [ ] Implémenter consume_from_redis() — accumuler les events sur 5 min
    [ ] Implémenter validate_events() — champs obligatoires, envoyer invalides en DLQ
    [ ] Implémenter enrich_events() — joindre avec le catalogue (track_id → artiste, genre)
    [ ] Implémenter store_to_parquet() — Parquet sur MinIO partitionné par heure
    [ ] Implémenter upsert_to_postgres() — insérer dans listening_events
    [ ] Utiliser TaskFlow API (@task) pour toutes les tâches
    [ ] Ajouter des branches conditionnelles : séparer listening_events et p2p_network_events
    [ ] Ajouter doc_md sur ce DAG
"""

import json
import logging
import os
import sys
import tempfile
import time
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow import DAG
from airflow.decorators import task

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.transformations.events import is_valid_listening_event
DAG_DOC = """
## streaming_events_pipeline

### Rôle
Consomme en micro-batch les événements du simulateur P2P depuis Redis,
les valide, les enrichit et les stocke en dual : Parquet (MinIO) + PostgreSQL.

### Sources
- Redis channel `listening_events`
- Redis channel `p2p_network_events`

### Destinations
- Table `listening_events` (PostgreSQL)
- Fichiers Parquet partitionnés sur MinIO : `s3://spotify-parquet/listening_events/date=.../hour=.../`
- Table `dead_letter_events` (pour les events invalides)

### Idempotence
    Chaque event est identifié par `event_id` (UUID). L'upsert utilise
    `ON CONFLICT (id) DO NOTHING` pour éviter les doublons.

### Traitement P2P
Les événements `p2p_network_events` sont validés et routés dans une branche
distincte pour préserver la séparation logique demandée par l'issue.
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           2,
    "retry_delay":       timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),
}

POSTGRES_CONN_ID = "spotify_postgres"
REDIS_CHANNELS = ["listening_events", "p2p_network_events"]
MINIO_BUCKET = "spotify-parquet"
BATCH_WINDOW_SEC = int(os.getenv("STREAMING_BATCH_WINDOW_SEC", "300"))
REDIS_POLL_SLEEP_SEC = float(os.getenv("STREAMING_REDIS_POLL_SLEEP_SEC", "0.2"))


def _normalize_timestamp(value: Any) -> str:
    if isinstance(value, datetime):
        timestamp = value
    else:
        timestamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))

    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    else:
        timestamp = timestamp.astimezone(timezone.utc)

    return timestamp.isoformat().replace("+00:00", "Z")


def _parse_timestamp(value: Any) -> datetime:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)


def _write_dlq_record(original_topic: str, error_type: str, payload: Any, error_message: str | None = None) -> None:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    connection = hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(
        """
        INSERT INTO dead_letter_events
            (id, original_topic, error_type, payload, error_message, created_at)
        VALUES (%s, %s, %s, %s::jsonb, %s, NOW())
        """,
        (
            str(uuid.uuid4()),
            original_topic,
            error_type,
            json.dumps(payload, ensure_ascii=False),
            error_message,
        ),
    )
    connection.commit()
    cursor.close()
    connection.close()


def _insert_dlq_records(records: list[dict[str, Any]]) -> None:
    if not records:
        return

    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    connection = hook.get_conn()
    cursor = connection.cursor()
    payloads = [
        (
            str(uuid.uuid4()),
            record["original_topic"],
            record["error_type"],
            json.dumps(record["payload"], ensure_ascii=False),
            record.get("error_message"),
        )
        for record in records
    ]
    cursor.executemany(
        """
        INSERT INTO dead_letter_events
            (id, original_topic, error_type, payload, error_message, created_at)
        VALUES (%s, %s, %s, %s::jsonb, %s, NOW())
        """,
        payloads,
    )
    connection.commit()
    cursor.close()
    connection.close()


with DAG(
    dag_id="streaming_events_pipeline",
    default_args=DEFAULT_ARGS,
    description="Micro-batch : Redis → validation → enrichissement → MinIO + PostgreSQL",
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "events", "streaming"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="consume_from_redis")
    def consume_from_redis(**context) -> dict:
        """
        Consomme les événements Redis publiés pendant la fenêtre de 5 minutes.

        TODO :
            1. Se connecter à Redis (REDIS_URL depuis les env vars)
            2. Utiliser un pattern subscriber ou lire depuis une liste Redis
               (le simulateur publie sur les channels REDIS_CHANNELS)
            3. Accumuler tous les messages de la fenêtre temporelle
            4. Retourner {"listening": [...], "p2p_network": [...]}

        Hint : avec redis pub/sub, les messages ne sont pas persistés.
        Une alternative : le simulateur peut aussi écrire dans une Redis LIST
        (lpush) que le DAG consomme avec rpop/lrange.
        Discutez avec l'équipe Infra & P2P de la stratégie choisie.
        """
        import redis

        redis_url = os.getenv("REDIS_URL", "redis://redis:6379/1")
        collected = {"listening": [], "p2p_network": []}

        try:
            client = redis.from_url(redis_url, decode_responses=True)
            client.ping()
            pubsub = client.pubsub()
            pubsub.subscribe(*REDIS_CHANNELS)

            deadline = time.time() + BATCH_WINDOW_SEC
            while time.time() < deadline:
                message = pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message is None:
                    time.sleep(REDIS_POLL_SLEEP_SEC)
                    continue

                channel = message.get("channel")
                data = message.get("data")
                if channel not in REDIS_CHANNELS or data is None:
                    continue

                try:
                    event = json.loads(data)
                except json.JSONDecodeError:
                    _write_dlq_record(
                        original_topic=str(channel),
                        error_type="invalid_json",
                        payload={"raw": data},
                        error_message="Message Redis non décodable en JSON",
                    )
                    continue

                if channel == "listening_events":
                    collected["listening"].append(event)
                else:
                    collected["p2p_network"].append(event)

            pubsub.close()
        except Exception as exc:
            logging.exception("Erreur pendant la consommation Redis")
            _write_dlq_record(
                original_topic="redis_pub_sub",
                error_type="redis_consume_error",
                payload={"channels": REDIS_CHANNELS},
                error_message=str(exc),
            )

        return collected

    @task(task_id="validate_events")
    def validate_events(raw_events: dict, **context) -> dict:
        """
        Valide les événements et isole les invalides en DLQ.

        Champs obligatoires pour un listening_event :
            event_id, user_id, track_id, timestamp, duration_ms

        TODO :
            1. Parcourir raw_events["listening"] et raw_events["p2p_network"]
            2. Valider les champs obligatoires
            3. Valider les types (timestamp parseable, duration_ms > 0)
            4. Invalides → INSERT dans dead_letter_events avec error_type="validation"
            5. Retourner {"valid_listening": [...], "valid_p2p": [...], "errors": N}
        """
        valid_listening: list[dict[str, Any]] = []
        valid_p2p: list[dict[str, Any]] = []
        dlq_records: list[dict[str, Any]] = []

        listening_required = {"event_id", "user_id", "track_id", "timestamp", "duration_ms"}
        p2p_required = {"event_id", "event_type", "peer_id", "timestamp"}
        allowed_p2p_types = {"peer_connect", "peer_disconnect", "chunk_transfer", "cache_hit", "cache_miss"}

        def validate_common(event: dict[str, Any], required_fields: set[str]) -> tuple[bool, str | None]:
            missing = sorted(field for field in required_fields if field not in event or event.get(field) in (None, ""))
            if missing:
                return False, f"Champs obligatoires manquants: {', '.join(missing)}"

            try:
                _normalize_timestamp(event["timestamp"])
            except Exception:
                return False, "timestamp non parseable"

            return True, None

        for event in raw_events.get("listening", []):
            if is_valid_listening_event(event):
                normalized = dict(event)
                normalized["timestamp"] = _normalize_timestamp(normalized["timestamp"])
                normalized["duration_ms"] = int(normalized["duration_ms"])
                normalized["completed"] = bool(normalized.get("completed", normalized["duration_ms"] > 30000))
                valid_listening.append(normalized)
            else:
                dlq_records.append({
                    "original_topic": "listening_events",
                    "error_type": "validation",
                    "payload": event,
                    "error_message": "Invalid listening event",
                })

        for event in raw_events.get("p2p_network", []):
            ok, error_message = validate_common(event, p2p_required)
            if ok and event.get("event_type") not in allowed_p2p_types:
                ok = False
                error_message = f"event_type invalide: {event.get('event_type')}"

            if ok:
                normalized = dict(event)
                normalized["timestamp"] = _normalize_timestamp(normalized["timestamp"])
                valid_p2p.append(normalized)
            else:
                dlq_records.append(
                    {
                        "original_topic": "p2p_network_events",
                        "error_type": "validation",
                        "payload": event,
                        "error_message": error_message,
                    }
                )

        _insert_dlq_records(dlq_records)

        return {
            "valid_listening": valid_listening,
            "valid_p2p": valid_p2p,
            "errors": len(dlq_records),
        }

    @task(task_id="route_p2p_network_events")
    def route_p2p_network_events(validated: dict, **context) -> dict:
        """
        Branche logique séparée pour les événements P2P.

        Dans cette itération, les événements P2P sont validés et observés,
        mais pas persistés dans une table dédiée.
        """
        p2p_events = validated.get("valid_p2p", [])
        summary: dict[str, int] = defaultdict(int)
        for event in p2p_events:
            summary[event.get("event_type", "unknown")] += 1

        logging.info("Branche P2P: %s événements valides", len(p2p_events))
        return {"count": len(p2p_events), "by_type": dict(summary)}

    @task(task_id="enrich_events")
    def enrich_events(validated: dict, **context) -> list:
        """
        Enrichit les événements d'écoute avec les données du catalogue.

        TODO :
            1. Charger les tracks depuis PostgreSQL (batch query par track_id)
               SELECT id, title, artist_id, genre FROM tracks WHERE id = ANY(%(ids)s)
            2. Pour chaque listening_event, ajouter : genre, artist_id, track_title
            3. Les track_id inconnus → DLQ avec error_type="unknown_track"
            4. Retourner la liste des events enrichis

        Hint : faire une seule requête PostgreSQL avec IN clause plutôt qu'une par event.
        """
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        listening_events = validated.get("valid_listening", [])
        if not listening_events:
            return []

        track_ids = sorted({event["track_id"] for event in listening_events})
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        connection = hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT id, title, artist_id, genre
            FROM tracks
            WHERE id = ANY(%s::uuid[])
            """,
            (track_ids,),
        )
        track_rows = cursor.fetchall()
        cursor.close()
        connection.close()

        track_map = {
            str(track_id): {
                "track_title": title,
                "artist_id": str(artist_id),
                "genre": genre,
            }
            for track_id, title, artist_id, genre in track_rows
        }

        enriched_events: list[dict[str, Any]] = []
        dlq_records: list[dict[str, Any]] = []

        for event in listening_events:
            track = track_map.get(event["track_id"])
            if track is None:
                dlq_records.append(
                    {
                        "original_topic": "listening_events",
                        "error_type": "unknown_track",
                        "payload": event,
                        "error_message": f"track_id inconnu: {event['track_id']}",
                    }
                )
                continue

            enriched_event = dict(event)
            enriched_event["source_peer_id"] = event.get("source_peer")
            enriched_event.update(track)
            enriched_events.append(enriched_event)

        _insert_dlq_records(dlq_records)
        return enriched_events

    @task(task_id="store_to_parquet")
    def store_to_parquet(enriched_events: list, **context) -> str:
        """
        Sauvegarde les événements enrichis en Parquet sur MinIO.

        Partitionnement : date + heure (pour la parallélisation Phase 1, seq 3.1)

        TODO :
            1. Convertir la liste d'events en DataFrame pandas
            2. Partitionner par date et heure du timestamp
            3. Écrire en Parquet sur MinIO via boto3 ou pyarrow
               Chemin : s3://spotify-parquet/listening_events/date={date}/hour={hour}/part-{run_id}.parquet
            4. Retourner le chemin du fichier écrit

        Hint : pyarrow.parquet.write_table() + boto3 pour l'upload
        """
        import boto3
        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq
        from botocore.config import Config

        if not enriched_events:
            return ""

        dataframe = pd.DataFrame(enriched_events)
        dataframe["timestamp_dt"] = pd.to_datetime(dataframe["timestamp"], utc=True)
        dataframe["date"] = dataframe["timestamp_dt"].dt.strftime("%Y-%m-%d")
        dataframe["hour"] = dataframe["timestamp_dt"].dt.strftime("%H")

        run_id = context.get("run_id", f"manual_{uuid.uuid4().hex}")
        minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
        access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")

        client = boto3.client(
            "s3",
            endpoint_url=minio_endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        )

        try:
            client.head_bucket(Bucket=MINIO_BUCKET)
        except Exception:
            client.create_bucket(Bucket=MINIO_BUCKET)

        uploaded_keys: list[str] = []
        for (date_value, hour_value), partition_frame in dataframe.groupby(["date", "hour"], dropna=False):
            object_key = f"listening_events/date={date_value}/hour={hour_value}/part-{run_id}.parquet"
            tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
            tmp_file_path = tmp_file.name
            tmp_file.close()

            table = pa.Table.from_pandas(
                partition_frame.drop(columns=["timestamp_dt"]),
                preserve_index=False,
            )
            pq.write_table(table, tmp_file_path)
            client.upload_file(tmp_file_path, MINIO_BUCKET, object_key)
            uploaded_keys.append(f"s3://{MINIO_BUCKET}/{object_key}")

            try:
                os.remove(tmp_file_path)
            except OSError:
                pass

        logging.info("Parquet écrit sur MinIO: %s", uploaded_keys)
        return uploaded_keys[0]

    @task(task_id="upsert_to_postgres")
    def upsert_to_postgres(enriched_events: list, **context) -> dict:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        if not enriched_events:
            return {"inserted": 0, "skipped": 0}

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        connection = hook.get_conn()
        cursor = connection.cursor()

        # Upsert des peers simulés avant l'insert des événements
        peer_rows = list({
            event.get("source_peer_id") or event.get("source_peer")
            for event in enriched_events
        } - {None})
        if peer_rows:
            cursor.executemany(
                """
                INSERT INTO peers (id, peer_name, status)
                VALUES (%s, %s, 'online')
                ON CONFLICT (id) DO NOTHING
                """,
                [(peer_id, f"peer-{peer_id[:8]}") for peer_id in peer_rows],
            )

        rows = [
            (
                event["event_id"],
                event["user_id"],
                event["track_id"],
                event.get("source_peer_id") or event.get("source_peer"),
                _parse_timestamp(event["timestamp"]).replace(tzinfo=None),
                int(event["duration_ms"]),
                event.get("device_type"),
                event.get("geo_country"),
                bool(event.get("completed", False)),
                event.get("event_source", "p2p"),
            )
            for event in enriched_events
        ]

        cursor.executemany(
            """
            INSERT INTO listening_events
                (id, user_id, track_id, source_peer_id, timestamp, duration_ms,
                device_type, geo_country, completed, event_source)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
            """,
            rows,
        )
        inserted = cursor.rowcount if cursor.rowcount is not None and cursor.rowcount >= 0 else len(rows)
        connection.commit()
        cursor.close()
        connection.close()

        return {"inserted": inserted, "skipped": len(rows) - inserted}

    # ── Orchestration ─────────────────────────────────────────
    raw        = consume_from_redis()
    validated  = validate_events(raw)
    p2p_routed = route_p2p_network_events(validated)
    enriched   = enrich_events(validated)

    # Branche parallèle : P2P events sont traités indépendamment
    store_to_parquet(enriched)
    upsert_to_postgres(enriched)
