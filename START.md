Créer les JSONS : `python \src\data_generator\generate_catalog.py`

Tester l'ingestion des données :`docker exec data-pipelines-production-airflow-webserver-1 python /opt/airflow/dags/catalog_ingestion_pipeline.py`

Tester les évènements P2P : ``python -m src.p2p_simulator.simulator --peers 10 --rate 3`. Dans un autre terminal : `docker exec -it data-pipelines-production-redis-1 redis-cli subscribe listening_events`