"""Steps 3, 4 and 6: run the SQL files in a layer folder (staging, marts or kpi)."""
import logging

from . import config
from .load import get_connection

log = logging.getLogger(__name__)

SCHEMAS = {"staging": "staging", "marts": "marts", "kpi": "kpi"}


def run_sql_layer(layer: str) -> list:
    """Run every .sql file in include/sql/<layer>/ in file-name order.

    Files are numbered (01_, 02_ ...) so dependencies run first, e.g. dimensions before
    the fact table. Each file is one CREATE OR REPLACE statement, so re-running is safe.
    """
    con = get_connection()
    built = []
    try:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMAS[layer]}")
        for sql_file in sorted((config.SQL_DIR / layer).glob("*.sql")):
            log.info("Running %s", sql_file.name)
            con.execute(sql_file.read_text())
            built.append(sql_file.stem)
        return built
    finally:
        con.close()


def export_kpis() -> list:
    """Write each KPI table to include/exports/<table>.csv for Tableau / Power BI / Excel."""
    config.EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    con = get_connection()
    try:
        tables = [r[0] for r in con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'kpi' ORDER BY table_name"
        ).fetchall()]
        for table in tables:
            out = config.EXPORT_DIR / f"{table}.csv"
            con.execute(f"COPY kpi.{table} TO '{out}' (HEADER, DELIMITER ',')")
            log.info("Exported kpi.%s -> %s", table, out.name)
        return tables
    finally:
        con.close()
