"""
DAG : aggregation_pipeline
============================
Calcule les agrégats des dernières 24h après streaming_events_pipeline.
"""
from datetime import datetime, timedelta
import psycopg2
import psycopg2.extras
import os

from airflow import DAG
from airflow.decorators import task
from airflow.sensors.sql import SqlSensor
from airflow.sensors.external_task import ExternalTaskSensor

DAG_DOC = """
## aggregation_pipeline
### Rôle
Calcule les agrégats des dernières 24h (top tracks, stats artistes, métriques P2P).
### Destinations
- Table `daily_streams` : top 50 tracks
- Table `artist_stats` : streams + unique listeners par artiste
### Stratégie
Fenêtre glissante : NOW() - INTERVAL 24h. Idempotente via ON CONFLICT DO UPDATE.
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           2,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

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
    dag_id="aggregation_pipeline",
    default_args=DEFAULT_ARGS,
    description="Agrégats 24h : top tracks, stats artistes, métriques P2P",
    schedule_interval="0 4 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "aggregation"],
    doc_md=DAG_DOC,
) as dag:

    wait_for_streaming = ExternalTaskSensor(
        task_id="wait_for_streaming_events_dag",
        external_dag_id="streaming_events_pipeline",
        external_task_id=None,
        allowed_states=["success"],
        poke_interval=60,
        timeout=3600,
        mode="reschedule",
    )

    wait_for_events = SqlSensor(
        task_id="wait_for_listening_events_data",
        conn_id="spotify_postgres",
        sql="SELECT COUNT(*) FROM listening_events WHERE timestamp >= NOW() - INTERVAL '24 hours'",
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    @task(task_id="compute_top_tracks")
    def compute_top_tracks(**context) -> list:
        """Top 50 tracks des dernières 24h."""
        conn = get_pg_conn()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        cursor.execute("""
            SELECT
                track_id,
                COUNT(*)                        AS total_streams,
                COUNT(DISTINCT user_id)         AS unique_listeners,
                SUM(duration_ms)                AS total_duration_ms,
                ARRAY_AGG(DISTINCT geo_country) AS countries
            FROM listening_events
            WHERE timestamp >= NOW() - INTERVAL '24 hours'
              AND completed = TRUE
            GROUP BY track_id
            ORDER BY total_streams DESC
            LIMIT 50
        """)

        rows = cursor.fetchall()
        result = [dict(row) for row in rows]
        cursor.close()
        conn.close()

        print(f"[compute_top_tracks] {len(result)} tracks trouvés")
        return result

    @task(task_id="compute_artist_stats")
    def compute_artist_stats(**context) -> list:
        """Stats par artiste des dernières 24h."""
        conn = get_pg_conn()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        cursor.execute("""
            SELECT
                a.id                            AS artist_id,
                a.name                          AS artist_name,
                COUNT(le.id)                    AS total_streams,
                COUNT(DISTINCT le.user_id)      AS unique_listeners,
                MODE() WITHIN GROUP (ORDER BY le.track_id) AS top_track_id
            FROM listening_events le
            JOIN tracks t   ON le.track_id = t.id
            JOIN artists a  ON t.artist_id = a.id
            WHERE le.timestamp >= NOW() - INTERVAL '24 hours'
              AND le.completed = TRUE
            GROUP BY a.id, a.name
            ORDER BY total_streams DESC
        """)

        rows = cursor.fetchall()
        result = [dict(row) for row in rows]
        cursor.close()
        conn.close()

        print(f"[compute_artist_stats] {len(result)} artistes trouvés")
        return result

    @task(task_id="compute_p2p_metrics")
    def compute_p2p_metrics(**context) -> dict:
        """Métriques P2P des dernières 24h."""
        conn = get_pg_conn()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Taux de cache hit
        cursor.execute("""
            SELECT event_source, COUNT(*) AS total
            FROM listening_events
            WHERE timestamp >= NOW() - INTERVAL '24 hours'
            GROUP BY event_source
        """)
        source_rows = cursor.fetchall()
        total_events = sum(r["total"] for r in source_rows)
        cache_hits   = sum(r["total"] for r in source_rows if r["event_source"] == "cache")
        cache_hit_rate = round(cache_hits / total_events, 4) if total_events > 0 else 0

        # Peers actifs
        cursor.execute("""
            SELECT COUNT(DISTINCT source_peer_id) AS active_peers
            FROM listening_events
            WHERE timestamp >= NOW() - INTERVAL '24 hours'
        """)
        active_peers = cursor.fetchone()["active_peers"]

        # Distribution device
        cursor.execute("""
            SELECT device_type, COUNT(*) AS total
            FROM listening_events
            WHERE timestamp >= NOW() - INTERVAL '24 hours'
            GROUP BY device_type
            ORDER BY total DESC
        """)
        device_dist = {r["device_type"]: r["total"] for r in cursor.fetchall()}

        # Distribution pays
        cursor.execute("""
            SELECT geo_country, COUNT(*) AS total
            FROM listening_events
            WHERE timestamp >= NOW() - INTERVAL '24 hours'
            GROUP BY geo_country
            ORDER BY total DESC
            LIMIT 10
        """)
        country_dist = {r["geo_country"]: r["total"] for r in cursor.fetchall()}

        cursor.close()
        conn.close()

        metrics = {
            "total_events":   total_events,
            "cache_hit_rate": cache_hit_rate,
            "active_peers":   active_peers,
            "device_dist":    device_dist,
            "country_dist":   country_dist,
        }
        print(f"[compute_p2p_metrics] total={total_events} | cache_hit={cache_hit_rate} | peers={active_peers}")
        return metrics

    @task(task_id="update_aggregates")
    def update_aggregates(top_tracks: list, artist_stats: list, p2p_metrics: dict, **context):
        """Écrit les agrégats dans PostgreSQL de façon idempotente."""
        today = datetime.utcnow().date()
        conn = get_pg_conn()
        cursor = conn.cursor()

        # UPSERT daily_streams
        for track in top_tracks:
            cursor.execute("""
                INSERT INTO daily_streams
                    (track_id, date, total_streams, unique_listeners,
                     total_duration_ms, countries, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (track_id, date) DO UPDATE SET
                    total_streams     = EXCLUDED.total_streams,
                    unique_listeners  = EXCLUDED.unique_listeners,
                    total_duration_ms = EXCLUDED.total_duration_ms,
                    countries         = EXCLUDED.countries,
                    updated_at        = NOW()
            """, (
                track["track_id"], today,
                track["total_streams"], track["unique_listeners"],
                track["total_duration_ms"], track["countries"],
            ))

        # UPSERT artist_stats
        for artist in artist_stats:
            cursor.execute("""
                INSERT INTO artist_stats
                    (artist_id, date, total_streams, unique_listeners,
                     top_track_id, updated_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (artist_id, date) DO UPDATE SET
                    total_streams    = EXCLUDED.total_streams,
                    unique_listeners = EXCLUDED.unique_listeners,
                    top_track_id     = EXCLUDED.top_track_id,
                    updated_at       = NOW()
            """, (
                artist["artist_id"], today,
                artist["total_streams"], artist["unique_listeners"],
                artist["top_track_id"],
            ))

        conn.commit()
        cursor.close()
        conn.close()

        print(f"[update_aggregates] {len(top_tracks)} tracks → daily_streams")
        print(f"[update_aggregates] {len(artist_stats)} artistes → artist_stats")
        print(f"[update_aggregates] métriques P2P : {p2p_metrics}")

        if top_tracks:
            print(f"[update_aggregates] Top track : {top_tracks[0]['track_id']} "
                  f"avec {top_tracks[0]['total_streams']} streams")

    # ── Orchestration ──────────────────────────────────────────
    top_tracks_result   = compute_top_tracks()
    artist_stats_result = compute_artist_stats()
    p2p_metrics_result  = compute_p2p_metrics()

    # Phase 1-3 : ExternalTaskSensor + SqlSensor pour robustesse
    wait_for_streaming >> wait_for_events >> [top_tracks_result, artist_stats_result, p2p_metrics_result]

    update_aggregates(top_tracks_result, artist_stats_result, p2p_metrics_result)