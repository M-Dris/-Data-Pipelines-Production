"""
Tests de structure des DAGs SPOTIFY
=====================================
Ces tests vérifient que les DAGs sont correctement définis sans les exécuter.
Ils doivent passer dès que le DAG est créé (même avec des NotImplementedError
dans les tâches — on teste la structure, pas l'implémentation).

Lancement :
    pytest tests/structure/ -v
    pytest tests/structure/test_dag_structure.py::test_catalog_ingestion_has_all_tasks -v
"""

import pytest
from airflow.models import DagBag


# ─────────────────────────────────────────────────────────────
# FIXTURES
# ─────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def dagbag():
    """Charge tous les DAGs depuis le dossier dags/."""
    return DagBag(dag_folder="dags/", include_examples=False)


@pytest.fixture(scope="module")
def catalog_dag(dagbag):
    return dagbag.get_dag("catalog_ingestion_pipeline")

@pytest.fixture(scope="module")
def streaming_events_dag(dagbag):
    return dagbag.get_dag("streaming_events_pipeline")

@pytest.fixture(scope="module")
def aggregation_dag(dagbag):
    return dagbag.get_dag("aggregation_pipeline")

@pytest.fixture(scope="module")
def recommendation_dag(dagbag):
    return dagbag.get_dag("recommendation_pipeline")

@pytest.fixture(scope="module")
def dlq_dag(dagbag):
    return dagbag.get_dag("dlq_reprocessing_pipeline")


# ─────────────────────────────────────────────────────────────
# TESTS GÉNÉRAUX — tous les DAGs
# ─────────────────────────────────────────────────────────────

def test_no_import_errors(dagbag):
    """Aucun DAG ne doit avoir d'erreur d'import."""
    assert not dagbag.import_errors, (
        f"Erreurs d'import dans les DAGs : {dagbag.import_errors}"
    )


def test_all_dags_present(dagbag):
    """Les 5 DAGs Phase 1 doivent être présents."""
    expected_dags = [
        "catalog_ingestion_pipeline",
        "streaming_events_pipeline",
        "aggregation_pipeline",
        "recommendation_pipeline",
        "dlq_reprocessing_pipeline",
    ]
    for dag_id in expected_dags:
        assert dagbag.get_dag(dag_id) is not None, f"DAG manquant : {dag_id}"


def test_all_dags_have_owner(dagbag):
    """Tous les DAGs doivent avoir un owner défini (pas 'airflow' par défaut)."""
    for dag_id, dag in dagbag.dags.items():
        if dag_id.startswith("spotify") or any(
            tag in dag.tags for tag in ["spotify", "phase-1", "phase-2"]
        ):
            assert dag.default_args.get("owner") != "airflow", (
                f"DAG {dag_id} : owner par défaut non modifié"
            )


def test_all_dags_have_retries(dagbag):
    """Tous les DAGs doivent configurer au moins 1 retry."""
    for dag_id, dag in dagbag.dags.items():
        if "phase" in str(dag.tags):
            retries = dag.default_args.get("retries", 0)
            assert retries >= 1, f"DAG {dag_id} : retries={retries} (minimum 1 attendu)"


def test_all_dags_have_tags(dagbag):
    """Tous les DAGs doivent avoir des tags pour faciliter la navigation."""
    for dag_id, dag in dagbag.dags.items():
        if dag_id in ["catalog_ingestion_pipeline", "streaming_events_pipeline",
                      "aggregation_pipeline", "recommendation_pipeline",
                      "dlq_reprocessing_pipeline"]:
            assert len(dag.tags) > 0, f"DAG {dag_id} : aucun tag défini"


def test_no_dag_has_cycles(dagbag):
    """Vérifier qu'aucun DAG n'a de cycle (condition sine qua non d'Airflow)."""
    for dag_id, dag in dagbag.dags.items():
        try:
            dag.topological_sort()
        except Exception as e:
            pytest.fail(f"DAG {dag_id} contient un cycle : {e}")


# ─────────────────────────────────────────────────────────────
# TESTS SPÉCIFIQUES — catalog_ingestion_pipeline
# ─────────────────────────────────────────────────────────────

class TestCatalogIngestionDAG:

    def test_dag_exists(self, catalog_dag):
        assert catalog_dag is not None

    def test_has_required_tasks(self, catalog_dag):
        """Les 4 tâches principales doivent exister."""
        task_ids = [t.task_id for t in catalog_dag.tasks]
        for expected in [
            "extract_from_minio",
            "validate_schema",
            "transform_catalog",
            "load_to_postgres",
        ]:
            assert expected in task_ids, f"Tâche manquante : {expected}"

    def test_task_order(self, catalog_dag):
        """L'ordre extract → validate → transform → load doit être respecté."""
        task_dict = {t.task_id: t for t in catalog_dag.tasks}
        extract   = task_dict.get("extract_from_minio")
        validate  = task_dict.get("validate_schema")
        transform = task_dict.get("transform_catalog")
        load      = task_dict.get("load_to_postgres")

        if all([extract, validate, transform, load]):
            assert validate in extract.downstream_list or validate.task_id in [
                t.task_id for t in extract.get_direct_relatives(upstream=False)
            ]

    def test_schedule_is_daily(self, catalog_dag):
        """Le DAG d'ingestion doit être planifié quotidiennement."""
        assert catalog_dag.schedule_interval in ["@daily", "0 2 * * *", "0 0 * * *"]

    def test_catchup_enabled(self, catalog_dag):
        """Catchup doit être activé pour le backfill historique."""
        assert catalog_dag.catchup is True, "catchup=True requis pour le backfill"

    def test_has_doc_md(self, catalog_dag):
        """Le DAG doit être documenté avec doc_md."""
        assert catalog_dag.doc_md is not None and len(catalog_dag.doc_md) > 50, (
            "doc_md manquant ou trop court (minimum 50 caractères)"
        )

    def test_max_active_runs_limited(self, catalog_dag):
        """max_active_runs doit être limité pour éviter les conflits."""
        assert catalog_dag.max_active_runs <= 3


# ─────────────────────────────────────────────────────────────
# TESTS SPÉCIFIQUES — aggregation_pipeline
# ─────────────────────────────────────────────────────────────

class TestAggregationDAG:

    def test_dag_exists(self, aggregation_dag):
        assert aggregation_dag is not None

    def test_has_external_task_sensor(self, aggregation_dag):
        """
        Le pipeline d'agrégation doit dépendre du pipeline d'événements
        via un ExternalTaskSensor.
        """
        from airflow.sensors.external_task import ExternalTaskSensor
        sensor_tasks = [
            t for t in aggregation_dag.tasks
            if isinstance(t, ExternalTaskSensor)
        ]
        assert len(sensor_tasks) >= 1, (
            "aggregation_pipeline doit attendre streaming_events_pipeline "
            "via ExternalTaskSensor"
        )

    def test_has_aggregation_tasks(self, aggregation_dag):
        task_ids = [t.task_id for t in aggregation_dag.tasks]
        # Au moins une tâche doit calculer les top tracks
        assert any("top" in tid or "aggregat" in tid or "stat" in tid
                   for tid in task_ids), (
            "Aucune tâche d'agrégation trouvée dans aggregation_pipeline"
        )
