# Interview prep: Loan Portfolio Analytics Pipeline

## 1. Walk through the code, step by step

Read the files in this order. Each step is one Airflow task.

| # | Task | File | What it does, in one breath |
|---|---|---|---|
| 1 | `extract_loans` | `include/lending_pipeline/extract.py` | Downloads the CSV to a `.part` file and renames it when done, so a failed download never leaves half a file. Then checks the header still has the 20 columns we depend on (**schema-drift check**). Skips the download if the file is cached. |
| 2 | `load_raw` | `include/lending_pipeline/load.py` | Loads the CSV into `raw.loans` with **every column as text** (`all_varchar`), plus a `_loaded_at` timestamp. Also loads the two seed CSVs. Raw is never modified, so it is always possible to re-run from it. |
| 3 | `build_staging` | `include/sql/staging/01_stg_loans.sql` | Casts types, renames to snake_case, unifies two grading systems into `grade_code`, maps statuses to 5 business buckets, and **deduplicates** with `ROW_NUMBER() OVER (PARTITION BY listing_key ORDER BY prosper_score)`. |
| 4 | `build_star_schema` | `include/sql/marts/01-06_*.sql` | Builds the 5 dimensions first, then `fact_loan`. File numbering controls the order. |
| 5 | `run_quality_checks` | `include/lending_pipeline/quality.py` | Runs 17 SQL checks, each returning the count of bad rows. Writes results to `audit.dq_results` and raises an error if a blocking check fails, which **fails the Airflow task**. |
| 6 | `build_kpis` | `include/sql/kpi/01-06_*.sql` | Builds 6 KPI tables. Only runs if step 5 passed. |
| 7 | `export_kpis` | `include/lending_pipeline/transform.py` | `COPY` each KPI table to CSV for Tableau / Power BI. |

**The DAG file** (`dags/lending_portfolio_pipeline.py`) uses the TaskFlow API (`@dag`, `@task`). Passing
each task's return value into the next both shares a small result through XCom and sets the
execution order. Settings worth knowing:
`schedule="@daily"`, `catchup=False` (full refresh, so there's nothing to backfill), `retries=2` with a
5-minute delay (handles a flaky download), and `max_active_runs=1` (DuckDB allows one writer at a time).

### The SCD Type 2 SQL (`02_dim_borrower.sql`), explained
1. **snapshots**: every loan listing is a snapshot of the borrower's profile at that moment.
2. **changes**: `LAG()` compares each snapshot with the previous one for the same borrower. If income,
   employment, occupation or home ownership changed, set `is_new_version = 1`.
3. **versioned**: a running `SUM()` of those flags gives a version number to each snapshot.
4. **collapsed**: `GROUP BY member_key, version_number` turns runs of identical snapshots into one row,
   with `valid_from` = the first snapshot time.
5. **final**: `valid_to` = `LEAD(valid_from)` (the next version's start), or `9999-12-31` for the current
   version. `is_current` = no next version.

The fact table then joins with `listing_created_at >= valid_from AND listing_created_at < valid_to`, a
**point-in-time join**.

---

## 2. Resume bullets

Pick 2-3. Each has one bold item, and every number comes from the project itself.

**Analyst / Business Finance version**
- Built a daily **Apache Airflow** pipeline in Python and SQL that models 113.9K loan records into a star schema (1 fact, 5 dimensions) and refreshes 6 credit-risk KPI tables for vintage, grade and segment analysis
- Analyzed matured-loan default rates with CTEs and window functions; found repeat borrowers default **~40% less** than first-time borrowers in the same credit grade at nearly the same interest rate, and recommended pricing on repayment history
- Automated **17 data quality checks** (reconciliation, uniqueness, referential integrity, SCD2 overlap) that stop KPI refresh on failure, and removed 871 duplicate source records in staging

**BI Engineer version**
- Designed a **star schema** with an SCD Type 2 borrower dimension and point-in-time joins over 113.9K loans, orchestrated daily with Apache Airflow (Astro CLI) on DuckDB
- Automated **17 SQL data quality checks** with results logged to an audit table, blocking dashboard refresh when reconciliation, key or referential integrity checks fail
- Delivered 6 KPI tables and **CSV exports** for Tableau / Power BI covering monthly originations (MoM, YoY, rolling 3-month), vintage default curves and risk-adjusted return by grade

(Tech line for the project header: *Apache Airflow, Astro CLI, Docker, Python, SQL, DuckDB, pandas, matplotlib*.)

---

## 3. Talking points

### "Why Airflow? Why not a cron job or a Python script?"
- **Dependencies:** KPIs must not build until the checks pass. Airflow models that as a graph, and a
  failed task stops everything downstream automatically.
- **Retries and failure handling:** `retries=2` covers a flaky download without anyone waking up.
- **Observability:** the UI shows each run, its duration, and logs per task, including every quality-check
  result. Cron offers none of this.
- **Scheduling and backfills:** if the source became daily files, `catchup` and the `{{ ds }}` date
  would allow re-processing any past day.
- **Why the Astro CLI?** One command (`astro dev start`) runs the whole Airflow stack in Docker, and the
  same project deploys to Astronomer's managed Airflow.
- **Link to day job:** "At Tredence I build PySpark/Delta pipelines on Databricks for a Teradata migration.
  This project shows the orchestration layer and the dimensional modeling on top."

### "How did you choose the schema?"
- **Grain first:** one row per loan, because every question (default rate, volume, return) is about loans.
- **Dimensions = how the business slices it:** time, credit grade, geography, purpose and borrower.
- **Facts = numbers you add up:** amount, interest collected, losses.
- **Star rather than one wide table:** each attribute is defined once (for example, risk band mapping),
  BI tools understand it natively, and dimensions are reusable.
- **SCD2 only where history matters:** borrower attributes change, and using today's income for a
  2010 loan would be wrong. Grade, state and purpose are static lookups, so plain tables are enough.
- **Unknown members** (`'XX'` state, `'NA'` grade) so no loan disappears in an inner join.

### "What does each quality check catch?" (see also the README table)
- `fact_row_count_matches_staging`: the most useful one. It catches a **join fan-out** (for example, if SCD2
  versions overlapped, a loan would match two borrower rows and be double-counted) or a dropped loan.
- `staging_matches_distinct_raw_listings`: proves the dedupe removed exactly the duplicates and nothing else.
- `fk_*` (5 checks): orphan keys. A new grade code from the source would show up here instead of
  silently disappearing from a dashboard.
- `scd2_*`: exactly one current version per borrower, and no overlapping date ranges.
- `loan_amount_and_rate_in_range` and `loan_status_accepted_values`: catch upstream format changes (for example,
  rates sent as 15.8 instead of 0.158).
- **Warn vs error:** 2 loans show net loss a few cents above the loan amount (source rounding). Not
  worth blocking the business, but logged so it's visible.

### "Walk me through an insight." (problem → metric → insight → recommendation)
- **Problem:** Is the grade capturing all the risk?
- **Metric:** matured-loan default rate, first vs repeat borrower, **within the same grade** and era.
- **Insight:** repeat borrowers default ~40% less in grades B-D (D: 13.7% vs 23.3%) but pay nearly the same
  rate. The overall gap looks smaller (12.5% vs 16.4%) because repeat loans skew to riskier grades,
  which is a Simpson's paradox and the reason I compared within grade.
- **Recommendation:** add repayment history to pricing, then validate with an A/B test.

### "What was the hardest analytical decision?"
Using **matured loans only** for default rates. My first version used "resolved" loans (paid off or
defaulted). That is biased: defaults resolve early, while good loans stay "Current" for 3 years, so recent
periods looked far riskier than they were. Restricting to loans whose full term has passed fixed it,
and the grade results flipped from "negative returns" to "positive in every grade".

### "What would you do next?"
- Switch to **incremental** loads: `MERGE` on `loan_key` and an SCD2 merge for borrowers.
- Add **alerting**: an `on_failure_callback` that posts to Slack or email when a check fails.
- Move the warehouse to Postgres/Snowflake, and the SQL to **dbt** models with dbt tests.
- Add a dashboard (Tableau/Power BI) on the exported KPI tables.

### Likely follow-up questions
- *Difference between SCD Type 1 and Type 2?* Type 1 overwrites (no history). Type 2 adds a new row with
  validity dates.
- *Why a surrogate key on dim_borrower?* `member_key` is no longer unique once there are versions;
  `borrower_sk` identifies one version.
- *Is the pipeline idempotent?* Yes. Every step is `CREATE OR REPLACE` from raw, so a re-run gives the
  same result. The audit table is append-only by design, so each run's checks are kept.
- *Why DuckDB?* It's a columnar, serverless engine that is fast for analytics on a laptop, and the SQL
  ports to any warehouse.
- *ROW_NUMBER vs RANK vs DENSE_RANK?* ROW_NUMBER is always unique (used for dedupe and top-N per region).
  RANK leaves gaps after ties (used to rank grades).
