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