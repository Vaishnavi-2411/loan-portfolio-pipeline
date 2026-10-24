"""
Lending portfolio pipeline (daily).

extract_loans -> load_raw -> build_staging -> build_star_schema -> run_quality_checks
    -> build_kpis -> export_kpis

The DAG only orchestrates. All business logic lives in include/lending_pipeline (Python)
and include/sql (SQL), so each step can also be run and tested without Airflow
(see run_local.py).
"""
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

from include.lending_pipeline import extract, load, quality, transform

default_args = {
    "owner": "vaishnavi",
    "retries": 2,                          # retry transient failures (e.g. download timeout)
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="lending_portfolio_pipeline",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,               # full refresh each run, so no need to backfill old dates
    max_active_runs=1,           # DuckDB allows one writer at a time
    default_args=default_args,
    tags=["lending", "star-schema", "data-quality"],
    doc_md=__doc__,
)
def lending_portfolio_pipeline():

    @task
    def extract_loans() -> str:
        return extract.download_loans()

    @task
    def load_raw(_raw_file: str) -> int:
        return load.load_raw()

    @task
    def build_staging(_raw_rows: int) -> list:
        return transform.run_sql_layer("staging")

    @task
    def build_star_schema(_staging: list) -> list:
        return transform.run_sql_layer("marts")

    @task
    def run_quality_checks(_marts: list) -> dict:
        # Raises on any failed "error" check -> task fails -> KPI tables keep yesterday's data.
        return quality.run_quality_checks()

    @task
    def build_kpis(_dq: dict) -> list:
        return transform.run_sql_layer("kpi")

    @task
    def export_kpis(_kpis: list) -> list:
        return transform.export_kpis()

    # Passing each task's output to the next one both shares results (XCom)
    # and sets the order of execution.
    raw_file = extract_loans()
    raw_rows = load_raw(raw_file)
    staging = build_staging(raw_rows)
    marts = build_star_schema(staging)
    dq = run_quality_checks(marts)
    kpis = build_kpis(dq)
    export_kpis(kpis)


lending_portfolio_pipeline()
