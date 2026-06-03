"""
DAG : recommendation_pipeline
================================
Génère les recommandations personnalisées via collaborative filtering
et les stocke dans Redis + PostgreSQL.
"""
from datetime import datetime, timedelta
import json
import os
import psycopg2
import psycopg2.extras
import redis as redis_lib

from airflow import DAG
from airflow.decorators import task
from airflow.sensors.external_task import ExternalTaskSensor

DAG_DOC = """
## recommendation_pipeline
### Rôle
Génère un top-10 de recommandations par utilisateur actif
via collaborative filtering (similarité cosinus entre profils d'écoute).
### Destinations
- Redis : clé `reco:{user_id}` → liste de track_ids (TTL 24h)
- PostgreSQL : table `recommendations`
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           1,
    "retry_delay":       timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=45),
}

REDIS_URL        = "redis://redis:6379/1"
RECO_TTL_SECONDS = 86400
TOP_N_RECO       = 10
LOOKBACK_DAYS    = 7


def get_pg_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "spotify"),
        password=os.getenv("POSTGRES_PASSWORD", "spotify"),
        dbname=os.getenv("POSTGRES_DB", "spotify"),
        options="-c client_encoding=UTF8"
    )


with DAG(
    dag_id="recommendation_pipeline",
    default_args=DEFAULT_ARGS,
    description="Collaborative filtering → recommandations Redis + PostgreSQL",
    schedule_interval="0 5 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "recommendation", "ml"],
    doc_md=DAG_DOC,
) as dag:

    wait_for_aggregation = ExternalTaskSensor(
        task_id="wait_for_aggregation",
        external_dag_id="aggregation_pipeline",
        external_task_id=None,
        allowed_states=["success"],
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    @task(task_id="build_user_track_matrix")
    def build_user_track_matrix(**context) -> dict:
        """
        Construit la matrice user x track des écoutes des 7 derniers jours.
        Retourne un dict {user_id: {track_id: play_count}}
        Ne garde que les users avec >= 3 écoutes distinctes.
        """
        conn = get_pg_conn()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        cursor.execute("""
            SELECT user_id, track_id, COUNT(*) AS play_count
            FROM listening_events
            WHERE timestamp >= NOW() - INTERVAL '7 days'
              AND completed = TRUE
            GROUP BY user_id, track_id
        """)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        # Construire le dict {user_id: {track_id: play_count}}
        matrix = {}
        for row in rows:
            uid = str(row["user_id"])
            tid = str(row["track_id"])
            if uid not in matrix:
                matrix[uid] = {}
            matrix[uid][tid] = row["play_count"]

        # Garder uniquement les users avec >= 3 tracks distincts
        matrix = {
            uid: tracks
            for uid, tracks in matrix.items()
            if len(tracks) >= 3
        }

        print(f"[build_user_track_matrix] {len(matrix)} utilisateurs actifs")
        return matrix

    @task(task_id="compute_recommendations")
    def compute_recommendations(matrix_data: dict, **context) -> dict:
        """
        Calcule les recommandations par similarité cosinus (numpy only).
        """
        import numpy as np

        if not matrix_data:
            print("[compute_recommendations] Matrice vide, pas de recommandations")
            return {}

        users  = list(matrix_data.keys())
        tracks = list({t for tracks in matrix_data.values() for t in tracks})

        if len(users) < 2:
            print("[compute_recommendations] Pas assez d'utilisateurs")
            return {}

        # Construire la matrice numpy
        track_index = {t: i for i, t in enumerate(tracks)}
        matrix_np = np.zeros((len(users), len(tracks)))

        for i, uid in enumerate(users):
            for tid, count in matrix_data[uid].items():
                j = track_index[tid]
                matrix_np[i][j] = count

        # Similarité cosinus manuellement (sans sklearn)
        norms = np.linalg.norm(matrix_np, axis=1, keepdims=True)
        norms[norms == 0] = 1  # éviter division par zéro
        matrix_normalized = matrix_np / norms
        similarity = np.dot(matrix_normalized, matrix_normalized.T)

        recommendations = {}

        for i, uid in enumerate(users):
            already_heard = set(matrix_data[uid].keys())
            scores = {}

            for j, other_uid in enumerate(users):
                if i == j:
                    continue
                sim = similarity[i][j]
                if sim <= 0:
                    continue
                for tid, count in matrix_data[other_uid].items():
                    if tid not in already_heard:
                        scores[tid] = scores.get(tid, 0) + sim * count

            top_tracks = sorted(scores.items(), key=lambda x: x[1], reverse=True)
            recommendations[uid] = [
                {"track_id": tid, "score": round(float(score), 4)}
                for tid, score in top_tracks[:TOP_N_RECO]
            ]

        print(f"[compute_recommendations] {len(recommendations)} users avec recommandations")
        return recommendations

    @task(task_id="store_recommendations")
    def store_recommendations(recommendations: dict, **context) -> dict:
        """
        Stocke les recommandations dans Redis et PostgreSQL.
        Redis  : reco:{user_id} → JSON liste track_ids (TTL 24h)
        PostgreSQL : UPSERT dans recommendations
        """
        if not recommendations:
            print("[store_recommendations] Aucune recommandation à stocker")
            return {"users_with_recos": 0, "total_recommendations": 0}

        # ── Redis ──────────────────────────────────────────────
        redis_host = os.getenv("REDIS_HOST", "redis")
        redis_port = int(os.getenv("REDIS_PORT", "6379"))
        r = redis_lib.Redis(host=redis_host, port=redis_port, db=1, decode_responses=True)

        redis_count = 0
        try:
            for user_id, track_list in recommendations.items():
                track_ids = [t["track_id"] for t in track_list]
                r.setex(
                    f"reco:{user_id}",
                    RECO_TTL_SECONDS,
                    json.dumps(track_ids)
                )
                redis_count += 1
            print(f"[store_recommendations] {redis_count} clés Redis stockées")
        except Exception as e:
            print(f"[store_recommendations] Erreur Redis : {e}")

        # ── PostgreSQL ─────────────────────────────────────────
        conn = get_pg_conn()
        cursor = conn.cursor()
        pg_count = 0

        for user_id, track_list in recommendations.items():
            for item in track_list:
                cursor.execute("""
                    INSERT INTO recommendations (user_id, track_id, score, generated_at)
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (user_id, track_id) DO UPDATE SET
                        score        = EXCLUDED.score,
                        generated_at = NOW()
                """, (user_id, item["track_id"], item["score"]))
                pg_count += 1

        conn.commit()
        cursor.close()
        conn.close()

        result = {
            "users_with_recos":      len(recommendations),
            "total_recommendations": pg_count,
        }
        print(f"[store_recommendations] {result}")
        return result

    # ── Orchestration ──────────────────────────────────────────
    matrix          = build_user_track_matrix()
    recommendations = compute_recommendations(matrix)

    wait_for_aggregation >> matrix
    store_recommendations(recommendations)