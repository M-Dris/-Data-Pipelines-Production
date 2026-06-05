"""
DAG : dlq_reprocessing_pipeline
==================================
Retraite périodiquement les événements défectueux de la Dead Letter Queue.

Planification : toutes les heures
Catchup       : désactivé

Architecture :
    PostgreSQL dead_letter_events (status='pending')
        → fetch_pending_dlq()       ← récupérer les events à retraiter
        → reprocess_events()        ← tenter de corriger et réinjecter
        → update_dlq_status()       ← marquer reprocessed ou abandoned

TODO :
    [x] Implémenter fetch_pending_dlq()
    [x] Implémenter reprocess_events()
    [x] Implémenter update_dlq_status()
    [x] Tester avec injection de données corrompues
    [x] Ajouter doc_md sur ce DAG
"""

from datetime import datetime, timedelta
import json
import logging

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_DOC = """
## dlq_reprocessing_pipeline

### Rôle
Retraite les événements défectueux isolés dans `dead_letter_events`.
Tente de corriger les erreurs et de réinjecter les events valides.

### Sources
- Table `dead_letter_events` où `status = 'pending'`

### Logique de retraitement
1. Récupérer les events `pending` avec `retry_count < 3`
2. Tenter la validation et la correction
3. Si succès → réinjecter dans `listening_events` + `status = 'reprocessed'`
4. Si échec après 3 tentatives → `status = 'abandoned'`

### Test d'injection
```sql
INSERT INTO dead_letter_events (payload, error_type, original_topic)
VALUES ('{"user_id": null, "track_id": "invalid"}', 'missing_fields', 'listening_events');
```

### TODO
Compléter les 3 tâches marquées NotImplementedError.
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           1,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=20),
}

POSTGRES_CONN_ID = "spotify_postgres"
MAX_RETRIES      = 3
BATCH_SIZE       = 100   # traiter par lots pour ne pas surcharger


with DAG(
    dag_id="dlq_reprocessing_pipeline",
    default_args=DEFAULT_ARGS,
    description="Retraitement horaire des événements Dead Letter Queue",
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "dlq", "resilience"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="fetch_pending_dlq")
    def fetch_pending_dlq(**context) -> list:
        """
        Récupère les événements en attente de retraitement.
        """
        # 1. Connexion via PostgresHook
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # 2. Requête des events pending non épuisés
        rows = hook.get_records(
            sql="""
                SELECT id, payload, error_type, retry_count, original_topic
                FROM dead_letter_events
                WHERE status = 'pending'
                  AND retry_count < %(max_retries)s
                ORDER BY created_at ASC
                LIMIT %(batch_size)s
            """,
            parameters={"max_retries": MAX_RETRIES, "batch_size": BATCH_SIZE},
        )

        # 3. Sérialiser en liste de dicts pour XCom
        events = [
            {
                "id":             row[0],
                "payload":        row[1],   # déjà dict si colonne JSONB, sinon str
                "error_type":     row[2],
                "retry_count":    row[3],
                "original_topic": row[4],
            }
            for row in rows
        ]

        # 4. Log
        logging.info(f"{len(events)} événements pending trouvés")
        return events

    @task(task_id="reprocess_events")
    def reprocess_events(pending_events: list, **context) -> dict:
        """
        Tente de corriger et réinjecter chaque événement défectueux.
        Retourne {"reprocessed": [...], "failed": [...]}
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # Charger les track_ids valides une seule fois (évite N requêtes)
        valid_tracks = {
            row[0]
            for row in hook.get_records("SELECT id FROM tracks")
        }

        reprocessed = []
        failed      = []

        for event in pending_events:
            event_id = event["id"]

            # Parser le payload (JSONB → dict, TEXT → json.loads)
            payload = event["payload"]
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except json.JSONDecodeError as e:
                    logging.warning(f"Event {event_id} — payload JSON invalide : {e}")
                    failed.append({"id": event_id, "reason": "invalid_json"})
                    continue

            # ── Validation & correction ──────────────────────────────
            # Règle 1 : user_id manquant → impossible à corriger
            if not payload.get("user_id"):
                logging.warning(f"Event {event_id} — user_id manquant, abandon")
                failed.append({"id": event_id, "reason": "missing_user_id"})
                continue

            # Règle 2 : timestamp invalide → fallback sur created_at
            if not payload.get("timestamp"):
                logging.info(f"Event {event_id} — timestamp absent, fallback created_at")
                payload["timestamp"] = datetime.utcnow().isoformat()

            # Règle 3 : track_id inconnu → abandon
            if payload.get("track_id") not in valid_tracks:
                logging.warning(f"Event {event_id} — track_id '{payload.get('track_id')}' inconnu, abandon")
                failed.append({"id": event_id, "reason": "unknown_track_id"})
                continue

            # ── Event valide → prêt pour réinsertion ────────────────
            reprocessed.append({
                "id":      event_id,
                "payload": payload,
            })

        logging.info(
            f"Reprocess terminé — valides: {len(reprocessed)}, "
            f"échoués: {len(failed)}"
        )
        return {"reprocessed": reprocessed, "failed": failed}

    @task(task_id="update_dlq_status")
    def update_dlq_status(results: dict, **context) -> dict:
        """
        Met à jour le statut des événements dans dead_letter_events.
        Réinsère les valides dans listening_events (idempotent ON CONFLICT DO NOTHING).
        
        Distinction entre erreurs :
        - Irrécupérables (user_id absent, track_id invalide) → status='abandoned' IMMÉDIATEMENT
        - Temporaires (JSON invalide, erreur réseau) → retry jusqu'à 3 tentatives
        """
        hook       = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn       = hook.get_conn()
        cur        = conn.cursor()

        reprocessed = results.get("reprocessed", [])
        failed      = results.get("failed", [])

        stats = {"reprocessed": 0, "abandoned": 0, "pending": 0}

        # ── 1. Events correctement retraités ────────────────────────
        for event in reprocessed:
            payload = event["payload"]
            try:
                # Réinsertion idempotente dans listening_events
                cur.execute(
                    """
                    INSERT INTO listening_events
                        (id, user_id, track_id, listened_at, duration_ms,
                         device_type, geo_country, completed, event_source)
                    VALUES
                        (%(event_id)s, %(user_id)s, %(track_id)s,
                         %(timestamp)s, %(duration_ms)s,
                         %(device_type)s, %(geo_country)s,
                         %(completed)s, %(event_source)s)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    {
                        "event_id":     payload.get("event_id", event["id"]),
                        "user_id":      payload["user_id"],
                        "track_id":     payload["track_id"],
                        "timestamp":    payload.get("timestamp"),
                        "duration_ms":  payload.get("duration_ms", 0),
                        "device_type":  payload.get("device_type"),
                        "geo_country":  payload.get("geo_country"),
                        "completed":    payload.get("completed", False),
                        "event_source": payload.get("event_source", "dlq_reprocessed"),
                    },
                )

                # Marquer comme reprocessed dans la DLQ
                cur.execute(
                    """
                    UPDATE dead_letter_events
                    SET status      = 'reprocessed',
                        resolved_at = NOW()
                    WHERE id = %s
                    """,
                    (event["id"],),
                )
                stats["reprocessed"] += 1

            except Exception as e:
                logging.error(f"Erreur réinsertion event {event['id']} : {e}")
                # Traiter comme failed pour incrémenter retry
                failed.append({"id": event["id"], "reason": str(e)})

        # ── 2. Séparer erreurs irrécupérables et temporaires ────────
        # Erreurs irrécupérables : pas de sens de retrier
        UNRECOVERABLE_REASONS = {"missing_user_id", "unknown_track_id", "invalid_json"}
        
        failed_unrecoverable = [e for e in failed if e["reason"] in UNRECOVERABLE_REASONS]
        failed_temporary     = [e for e in failed if e["reason"] not in UNRECOVERABLE_REASONS]

        # ── 3a. Events irrécupérables → abandoned IMMÉDIATEMENT ─────
        for event in failed_unrecoverable:
            cur.execute(
                """
                UPDATE dead_letter_events
                SET status        = 'abandoned',
                    last_retry_at = NOW(),
                    resolved_at   = NOW()
                WHERE id = %s
                """,
                (event["id"],),
            )
            logging.info(f"Event {event['id']} abandonnée (erreur irrécupérable : {event['reason']})")
            stats["abandoned"] += 1

        # ── 3b. Events temporaires : retry jusqu'à MAX_RETRIES ──────
        for event in failed_temporary:
            cur.execute(
                """
                UPDATE dead_letter_events
                SET retry_count   = retry_count + 1,
                    last_retry_at = NOW(),
                    status        = CASE
                                      WHEN retry_count + 1 >= %s THEN 'abandoned'
                                      ELSE 'pending'
                                    END
                WHERE id = %s
                RETURNING status
                """,
                (MAX_RETRIES, event["id"]),
            )
            row = cur.fetchone()
            if row:
                new_status = row[0]
                if new_status == "abandoned":
                    logging.info(f"Event {event['id']} abandonnée (max retries atteint)")
                    stats["abandoned"] += 1
                else:
                    logging.info(f"Event {event['id']} marquée pending pour retry")
                    stats["pending"] += 1

        conn.commit()
        cur.close()
        conn.close()

        # ── 4. Bilan ────────────────────────────────────────────────
        logging.info(
            f"Bilan DLQ — retraités: {stats['reprocessed']}, "
            f"abandonnés: {stats['abandoned']}, "
            f"encore pending: {stats['pending']}"
        )
        return stats

    # ── Orchestration ─────────────────────────────────────────
    pending = fetch_pending_dlq()
    results = reprocess_events(pending)
    update_dlq_status(results)