"""Step 2: load the raw CSV and reference seeds into the warehouse, untouched."""
import logging

import duckdb

from . import config

log = logging.getLogger(__name__)


def get_connection() -> duckdb.DuckDBPyConnection:
    config.WAREHOUSE_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(config.WAREHOUSE_PATH))


def load_raw() -> int:
    """Full-refresh load of raw.loans plus the two seed tables. Returns the raw row count.

    Every column is loaded as text (all_varchar) so a bad value can never break the load;
    type casting happens in the staging SQL where it is visible and testable.
    """
    con = get_connection()
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")
        con.execute(
            """
            CREATE OR REPLACE TABLE raw.loans AS
            SELECT *, current_timestamp AS _loaded_at
            FROM read_csv(?, header = true, all_varchar = true)
            """,
            [str(config.RAW_CSV)],
        )
        for seed in ("loan_purpose", "state_region"):
            con.execute(
                f"CREATE OR REPLACE TABLE raw.{seed} AS SELECT * FROM read_csv(?, header = true)",
                [str(config.SEEDS_DIR / f"{seed}.csv")],
            )
        rows = con.execute("SELECT count(*) FROM raw.loans").fetchone()[0]
        log.info("Loaded %s rows into raw.loans", rows)
        return rows
    finally:
        con.close()
