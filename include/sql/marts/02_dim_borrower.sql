-- Borrower dimension, Slowly Changing Dimension Type 2.
-- 15.7k borrowers took more than one loan, and their income range, employment status,
-- occupation or home ownership often changed between loans. Type 2 keeps every version
-- with valid_from / valid_to, so each loan joins to the profile AS IT WAS when the borrower
-- applied (point-in-time), not to today's profile.
CREATE OR REPLACE TABLE marts.dim_borrower AS
WITH snapshots AS (
    -- Each loan listing is a snapshot of the borrower's profile at that moment.
    SELECT
        member_key,
        listing_created_at AS snapshot_at,
        income_range,
        employment_status,
        occupation,
        is_homeowner
    FROM staging.stg_loans
),
changes AS (
    -- Flag a new version whenever any tracked attribute differs from the previous snapshot.
    SELECT
        *,
        CASE
            WHEN lag(income_range) OVER w IS NULL THEN 1
            WHEN (income_range, employment_status, occupation, is_homeowner)
                 IS DISTINCT FROM
                 (lag(income_range) OVER w, lag(employment_status) OVER w,
                  lag(occupation) OVER w, lag(is_homeowner) OVER w) THEN 1
            ELSE 0
        END AS is_new_version
    FROM snapshots
    WINDOW w AS (PARTITION BY member_key ORDER BY snapshot_at)
),
versioned AS (
    -- Running total of change flags = version number for each snapshot.
    SELECT
        *,
        sum(is_new_version) OVER (
            PARTITION BY member_key ORDER BY snapshot_at
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS version_number
    FROM changes
),
collapsed AS (
    SELECT
        member_key,
        version_number,
        min(snapshot_at)        AS valid_from,
        any_value(income_range) AS income_range,
        any_value(employment_status) AS employment_status,
        any_value(occupation)   AS occupation,
        any_value(is_homeowner) AS is_homeowner
    FROM versioned
    GROUP BY member_key, version_number
)
SELECT
    row_number() OVER (ORDER BY member_key, version_number) AS borrower_sk,  -- surrogate key
    member_key,                                                               -- natural key
    version_number,
    income_range,
    employment_status,
    occupation,
    is_homeowner,
    valid_from,
    coalesce(
        lead(valid_from) OVER (PARTITION BY member_key ORDER BY version_number),
        TIMESTAMP '9999-12-31'
    ) AS valid_to,  -- exclusive upper bound
    lead(valid_from) OVER (PARTITION BY member_key ORDER BY version_number) IS NULL AS is_current
FROM collapsed;
