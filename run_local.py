"""Run the whole pipeline without Airflow (quick local check / debugging).

    python run_local.py

Runs the same functions, in the same order, as the Airflow DAG.
"""
import logging

from include.lending_pipeline import extract, load, quality, transform

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

if __name__ == "__main__":
    extract.download_loans()
    load.load_raw()
    transform.run_sql_layer("staging")
    transform.run_sql_layer("marts")
    print(quality.run_quality_checks())
    transform.run_sql_layer("kpi")
    transform.export_kpis()
