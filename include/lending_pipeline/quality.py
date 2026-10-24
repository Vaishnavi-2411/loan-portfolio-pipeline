"""Step 5: data quality checks. Runs after the star schema is built and BEFORE the KPI tables.

Each check is a SQL query that returns the number of BAD rows (0 = pass).
- severity "error": the Airflow task fails, so the KPI tables are not refreshed with bad data.
- severity "warn":  logged and stored, but the run continues.
Every result is saved to audit.dq_results so failures can be tracked over time.
"""
import logging
from datetime import datetime, timezone

from .load import get_connection

log = logging.getLogger(__name__)

CHECKS = [
    # --- completeness / volume ---
    ("raw_loans_not_empty", "error",
     "SELECT CASE WHEN count(*) = 0 THEN 1 ELSE 0 END FROM raw.loans"),
    ("staging_matches_distinct_raw_listings", "error",
     """SELECT abs((SELECT count(DISTINCT ListingKey) FROM raw.loans)
                 - (SELECT count(*) FROM staging.stg_loans))"""),
    ("fact_row_count_matches_staging", "error",
     """SELECT abs((SELECT count(*) FROM staging.stg_loans)
                 - (SELECT count(*) FROM marts.fact_loan))"""),

    # --- uniqueness ---
    ("fact_loan_key_unique", "error",
     "SELECT count(*) - count(DISTINCT loan_key) FROM marts.fact_loan"),
    ("dim_borrower_sk_unique", "error",
     "SELECT count(*) - count(DISTINCT borrower_sk) FROM marts.dim_borrower"),

    # --- not null ---
    ("fact_keys_not_null", "error",
     """SELECT count(*) FROM marts.fact_loan
        WHERE loan_key IS NULL OR borrower_sk IS NULL OR origination_date_key IS NULL
           OR grade_code IS NULL OR state_code IS NULL OR purpose_id IS NULL"""),

    # --- referential integrity (orphan foreign keys) ---
    ("fk_fact_to_dim_date", "error",
     """SELECT count(*) FROM marts.fact_loan f
        LEFT JOIN marts.dim_date d ON f.origination_date_key = d.date_key
        WHERE d.date_key IS NULL"""),
    ("fk_fact_to_dim_borrower", "error",
     """SELECT count(*) FROM marts.fact_loan f
        LEFT JOIN marts.dim_borrower b ON f.borrower_sk = b.borrower_sk
        WHERE b.borrower_sk IS NULL"""),
    ("fk_fact_to_dim_credit_grade", "error",
     """SELECT count(*) FROM marts.fact_loan f
        LEFT JOIN marts.dim_credit_grade g ON f.grade_code = g.grade_code
        WHERE g.grade_code IS NULL"""),
    ("fk_fact_to_dim_geography", "error",
     """SELECT count(*) FROM marts.fact_loan f
        LEFT JOIN marts.dim_geography g ON f.state_code = g.state_code
        WHERE g.state_code IS NULL"""),
    ("fk_fact_to_dim_loan_purpose", "error",
     """SELECT count(*) FROM marts.fact_loan f
        LEFT JOIN marts.dim_loan_purpose p ON f.purpose_id = p.purpose_id
        WHERE p.purpose_id IS NULL"""),

    # --- SCD Type 2 integrity ---
    ("scd2_one_current_row_per_borrower", "error",
     """SELECT count(*) FROM (
            SELECT member_key FROM marts.dim_borrower
            GROUP BY member_key HAVING sum(CASE WHEN is_current THEN 1 ELSE 0 END) <> 1)"""),
    ("scd2_no_overlapping_versions", "error",
     """SELECT count(*) FROM marts.dim_borrower a
        JOIN marts.dim_borrower b
          ON a.member_key = b.member_key AND a.borrower_sk < b.borrower_sk
         AND a.valid_from < b.valid_to AND b.valid_from < a.valid_to"""),

    # --- valid values / business rules ---
    ("loan_status_accepted_values", "error",
     """SELECT count(*) FROM marts.fact_loan
        WHERE loan_status_group NOT IN ('Current', 'Paid Off', 'Delinquent', 'Default', 'Cancelled')"""),
    ("loan_amount_and_rate_in_range", "error",
     """SELECT count(*) FROM marts.fact_loan
        WHERE loan_amount <= 0 OR borrower_rate NOT BETWEEN 0 AND 1
           OR term_months NOT IN (12, 36, 60)"""),
    ("loss_not_greater_than_loan_amount", "warn",
     "SELECT count(*) FROM marts.fact_loan WHERE net_principal_loss > loan_amount"),
    ("default_has_closed_date", "warn",
     "SELECT count(*) FROM marts.fact_loan WHERE is_default AND closed_date_key IS NULL"),
]


def run_quality_checks() -> dict:
    """Run all checks, store results in audit.dq_results, raise if any 'error' check fails."""
    con = get_connection()
    results = []
    run_at = datetime.now(timezone.utc).replace(tzinfo=None)  # one timestamp per run
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS audit")
        con.execute(
            """CREATE TABLE IF NOT EXISTS audit.dq_results (
                   run_at TIMESTAMP, check_name VARCHAR, severity VARCHAR,
                   failed_rows BIGINT, status VARCHAR)"""
        )
        for name, severity, sql in CHECKS:
            failed = con.execute(sql).fetchone()[0]
            status = "PASS" if failed == 0 else ("FAIL" if severity == "error" else "WARN")
            results.append((name, severity, failed, status))
            log.info("%-40s %-5s failed_rows=%s", name, status, failed)
            con.execute(
                "INSERT INTO audit.dq_results VALUES (?, ?, ?, ?, ?)",
                [run_at, name, severity, failed, status],
            )
    finally:
        con.close()

    failures = [r[0] for r in results if r[3] == "FAIL"]
    if failures:
        raise ValueError(f"Data quality checks failed: {failures}")
    return {
        "passed": sum(r[3] == "PASS" for r in results),
        "warnings": [r[0] for r in results if r[3] == "WARN"],
    }
