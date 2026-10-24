"""DAG integrity tests. Run with `astro dev pytest` (or plain pytest inside the container)."""
from airflow.models import DagBag

DAG_ID = "lending_portfolio_pipeline"


def get_dagbag():
    return DagBag(dag_folder="dags", include_examples=False)


def test_no_import_errors():
    assert get_dagbag().import_errors == {}


def test_task_order():
    dag = get_dagbag().dags[DAG_ID]
    expected = [
        "extract_loans", "load_raw", "build_staging", "build_star_schema",
        "run_quality_checks", "build_kpis", "export_kpis",
    ]
    assert [t.task_id for t in dag.topological_sort()] == expected


def test_retries_configured():
    dag = get_dagbag().dags[DAG_ID]
    assert all(t.retries >= 1 for t in dag.tasks)
