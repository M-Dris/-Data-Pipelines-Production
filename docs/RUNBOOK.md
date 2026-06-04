# RUNBOOK SPOTIFY — Procédures incidents

> Ce document doit être complété par votre groupe au fur et à mesure de la semaine.
> Un bon runbook = ce dont vous auriez eu besoin pendant la panne.

---

## Incidents Phase 1 — Airflow / Batch

### INC-01 — DAG bloqué en "running" depuis > 30 minutes

**Symptômes :** Une tâche reste en état `running` dans l'UI Airflow.

**Diagnostic :**
```bash
# Voir les logs de la tâche
docker compose logs airflow-worker -f

# Lister les tâches actives
docker exec airflow-scheduler airflow tasks states-for-dag-run <dag_id> <run_id>
```

**Résolution :**
```bash
# Marquer la tâche comme failed manuellement
docker exec airflow-scheduler airflow tasks clear <dag_id> -t <task_id> --yes

# Ou tuer le worker et le relancer
docker compose restart airflow-worker
```

**Cause probable :** → À compléter par votre groupe après avoir rencontré cet incident

---

### INC-02 — PostgreSQL : `too many connections`

**Symptômes :** Les tâches Airflow échouent avec `FATAL: too many connections`.

**Diagnostic :**
```sql
SELECT count(*), state FROM pg_stat_activity GROUP BY state;
SELECT max_conn FROM pg_settings WHERE name='max_connections';
```

**Résolution :**
```bash
# Augmenter max_connections dans docker-compose
# PostgreSQL environment: POSTGRES_MAX_CONNECTIONS: 200

# Court terme : killer les connexions idle
# SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE state='idle';
```

**Prévention :** → À compléter (hint : Airflow pools)

---

### INC-03 — MinIO inaccessible depuis Airflow

**Symptômes :** Les tâches de lecture/écriture Parquet échouent avec `Connection refused`.

**Diagnostic :**
```bash
docker compose ps minio
curl http://localhost:9000/minio/health/live
```

**Résolution :**
```bash
docker compose restart minio
# Attendre 10s puis relancer le DAGRun
```

---

## Incidents Phase 2 — Kafka / Spark

### INC-04 — Consumer lag Kafka qui explose

**Symptômes :** Kafka UI → consumer group `spark-streaming-trends` → lag > 10 000

**Diagnostic :**
```bash
# Vérifier le throughput Spark
docker logs spark-master -f | grep "Batch Duration"

# Vérifier les ressources
docker stats spark-worker-1
```

**Résolution :**
→ À compléter par votre groupe

---

### INC-05 — Job Spark crash avec OutOfMemory

**Symptômes :** `java.lang.OutOfMemoryError: GC overhead limit exceeded`

**Diagnostic :**
```bash
docker logs spark-master -f | grep -i "error\|exception\|oom"
```

**Résolution :**
```bash
# Augmenter la mémoire du worker dans docker-compose
# SPARK_WORKER_MEMORY: 4G

# Réduire le state store : ajouter un TTL sur flatMapGroupsWithState
# GroupState.setTimeoutDuration("1 hour")
```

---

### INC-06 — Spark ne reprend pas depuis le checkpoint

**Symptômes :** Après redémarrage, le job repart de zéro au lieu du checkpoint.

**Diagnostic :**
```bash
# Vérifier que le checkpoint est sur MinIO
docker exec data-pipelines-production-minio-1 mc ls local/spotify-checkpoints/streaming_trends/

# Vérifier les logs Spark au démarrage
docker logs spotify-spark-master | grep -i "checkpoint\|batch\|offset"
```

**Résolution :**
```bash
# 1. Vérifier que le checkpointLocation est bien configuré dans le job
#    → spark_jobs/streaming_trends_job.py : CHECKPOINT_PATH = "s3a://spotify-checkpoints/streaming_trends"

# 2. Si le checkpoint est corrompu, le supprimer et repartir de "earliest"
docker exec data-pipelines-production-minio-1 mc rm --recursive --force local/spotify-checkpoints/streaming_trends/
# Puis modifier startingOffsets en "earliest" pour rejouer les données

# 3. Relancer le job — il doit afficher "Batch: N" avec N > 0 (reprise depuis le dernier offset)
docker exec spotify-spark-master spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  /opt/spark-jobs/streaming_trends_job.py
```

**Prévention :** Le checkpoint MinIO garantit la reprise exactement là où le job s'est arrêté. Ne jamais modifier `checkpointLocation` entre deux exécutions du même job.

---

### INC-07 — Exactly-once : vérification des doublons après redémarrage

**Contexte :** Issue #16 — La chaîne exactly-once repose sur :
- Producteur idempotent (`enable.idempotence=True`, `transactional.id=p2p-simulator-1`)
- Consommateur Spark avec `isolation.level=read_committed` (ne lit que les messages committés)
- Checkpoint Spark sur MinIO (reprise des offsets exacts)

**Procédure de vérification :**

```bash
# 1. Lancer le job et noter le numéro du dernier batch affiché (ex: "Batch: 52")
docker exec spotify-spark-master spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  /opt/spark-jobs/streaming_trends_job.py

# 2. Arrêter le job (Ctrl+C) et attendre 2 minutes (le simulateur continue à produire)

# 3. Relancer le job → doit reprendre à "Batch: 53" (pas de retraitement des offsets déjà consommés)

# 4. Vérifier l'absence de doublons dans la table listening_events (peuplée par le DAG batch)
docker exec data-pipelines-production-postgres-1 psql -U spotify -d spotify -c \
  "SELECT COUNT(*) AS total, COUNT(DISTINCT id) AS uniques, COUNT(*) - COUNT(DISTINCT id) AS doublons FROM listening_events;"
```

**Résultat attendu :** `doublons = 0`

**Si des doublons apparaissent :**
```bash
# Vérifier que le simulateur utilise bien transactional.id
# → src/p2p_simulator/simulator.py : "transactional.id": "p2p-simulator-1"

# Vérifier que Spark lit en read_committed
# → spark_jobs/streaming_trends_job.py : .option("kafka.isolation.level", "read_committed")
```

---

## Chaos Engineering — Résultats

> Compléter pendant l'issue #25 (vendredi)

### Scénario 1 : Arrêt d'un broker Kafka

**Commande :** `docker compose stop kafka-2`

**Comportement observé :** ...

**Recovery automatique :** oui / non — détails : ...

**Temps de recovery :** ...

---

### Scénario 2 : Kill du driver Spark

**Commande :** `docker compose kill spark-master`

**Comportement observé :** ...

**Recovery depuis checkpoint :** oui / non — détails : ...

**Doublons introduits :** 0 / N — vérification : ...

---

### Scénario 3 : Coupure PostgreSQL

**Commande :** `docker compose stop postgres` (2 minutes) → `docker compose start postgres`

**Comportement observé (Airflow) :** ...

**Comportement observé (Spark) :** ...

**Données perdues :** oui / non — détails : ...
