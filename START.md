## Issue 1

`docker compose up -d`

## Issue 2

## Issue 3 

Créer les JSONS : `python src\data_generator\generate_catalog.py`

## Issue 4

Ingestion des données JSON dans le catalogue :`docker exec data-pipelines-production-airflow-webserver-1 python /opt/airflow/dags/catalog_ingestion_pipeline.py`


## Issue 5

Génération de données P2P : ``python -m src.p2p_simulator.simulator --peers 10 --rate 3`. Le simulateur doit pouvoir se connecter à la BDD pour trouver les titres déjà existant, si il y a une erreur de connexion.
Dans un autre terminal : `docker exec -it data-pipelines-production-redis-1 redis-cli subscribe listening_events`. Si les évènements se suivent en boucle, c'est ok.


## Issue 6

Aller dans Airflow et lancer le dag streaming_events_pipeline, attendre la fin de la task

Vérifier que les documents sont stocké dans listening_events : `docker-compose exec -T postgres psql -U spotify -d spotify -tA -c "SELECT COUNT(*) FROM listening_events;"`.
Vérifier qu'ils sont stockés dans Minio (http://localhost:9001/browser/spotify-parquet) 

## Issue 7

1 - Activer le dag aggrégation pipeline, attendre la fin de la task.

2 - Vérifier : `docker exec -it data-pipelines-production-postgres-1 psql -U spotify -d spotify -c "SELECT COUNT(*) FROM daily_streams; SELECT COUNT(*) FROM artist_stats;"`

Résultat attendu :
``` 
count 
-------
    50
(1 row)

 count 
-------
    30
(1 row)

```

## Issue 8

1 - Activer le dag recommandation pipeline, attendre la fin de la task.

2 - Vérifier que les reco ont été ajouté : `docker exec -it data-pipelines-production-redis-1 redis-cli -n 1 keys "reco:*"` (Ne doit pas être vide)

2 bis - `docker exec -it data-pipelines-production-postgres-1 psql -U spotify -d spotify -c "SELECT COUNT(*) FROM recommendations;"` (Ne doit pas être vide)

## Issue 9

1 - Ajout d'un event corrompues dans la BDD : `docker compose cp tests/dlq_test.sql postgres:/tmp/dlq_test.sql`

2 - Vérifier le status de l'event : `docker compose exec postgres psql -U spotify -d spotify -f /tmp/dlq_test.sql`
    
Résultat attendu : 

    ```INSERT 0 1
    status  | count
    ---------+-------
    pending |     1
    (1 row)```

3 - Lancer le DAG depuis Airflow (Bouton Play - Trigger DAG) attendre la fin de la task

4 - Vérifie l'état de l'évent : `docker compose exec postgres psql -U spotify -d spotify -c "SELECT id, status, retry_count FROM dead_letter_events;"`

Résultat attendu : `status : abandoned`

## Issue 10

Tout doit passer

1 - `pytest tests/structure/ -v`

2 - `pytest tests/unit/ -v`

3 - `pytest tests/ -v --tb=short`

## Issue 11

Vérifier la présence des 6 topics. http://localhost:8090

## Issue 12

Lancer le simulator.py `python -m src.p2p_simulator.simulator --peers 10 --rate 3`.

Vérifier dans Kafka UI l'augmentation de la quantité d'envent dans listening_events

## Issue 13

1 - Démarrer le cluster Spark : `docker compose up -d spark-master spark-worker-1`

2 - Vérifier que le worker est bien enregistré : `docker logs spotify-spark-master`
Résultat attendu : `Registering worker ... with 2 cores, 2.0 GiB RAM`

3 - Lancer le job en mode continu (processingTime) :
`docker exec spotify-spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /opt/spark-jobs/streaming_trends_job.py`

Les events JSON doivent apparaître dans la console toutes les 10 secondes. Le checkpoint est écrit dans MinIO (`s3a://spotify-checkpoints/streaming_trends`).

4 - Tester le mode Once (traite un seul batch puis s'arrête) : modifier `.trigger(processingTime="10 seconds")` en `.trigger(once=True)` dans `spark_jobs/streaming_trends_job.py`, relancer la même commande.
Résultat attendu : un seul batch affiché, puis le job se termine proprement.

5 - Vérifier le checkpoint dans MinIO : http://localhost:9001/browser/spotify-checkpoints

## Issue 16

1 - Lancer le simulateur et le job Spark (dans deux terminaux séparés) :
```
python -m src.p2p_simulator.simulator --peers 10 --rate 3
```
```
docker exec spotify-spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /opt/spark-jobs/streaming_trends_job.py
```

2 - Noter le numéro du dernier batch affiché (ex: `Batch: 52`), puis arrêter le job Spark (Ctrl+C). Attendre 2 minutes (le simulateur continue de produire).

3 - Relancer le job Spark. Résultat attendu : le job **ne repart pas de Batch 0** (checkpoint respecté). Il rattrape rapidement le backlog accumulé pendant l'arrêt (ex: saute de Batch 25 à Batch 45). C'est le comportement exactly-once normal — les offsets Kafka déjà consommés ne sont pas retraités.

4 - Avant d'arrêter le job, noter le count :
`docker exec data-pipelines-production-postgres-1 psql -U spotify -d spotify -c "SELECT COUNT(*) AS total, COUNT(DISTINCT event_id) AS uniques FROM streaming_event_ids;"`

5 - Arrêter le job (Ctrl+C), attendre 2 minutes, relancer (cf. commande étape 1).

6 - Vérifier l'absence de doublons après redémarrage :
`docker exec data-pipelines-production-postgres-1 psql -U spotify -d spotify -c "SELECT COUNT(*) AS total, COUNT(DISTINCT event_id) AS uniques, COUNT(*) - COUNT(DISTINCT event_id) AS doublons FROM streaming_event_ids;"`

Résultat attendu : `doublons = 0`, `total` > valeur avant arrêt (backlog rattrapé).