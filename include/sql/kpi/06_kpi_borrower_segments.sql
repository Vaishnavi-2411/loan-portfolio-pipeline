-- Borrower segments: first-time vs repeat borrowers, by lending era, credit grade and income.
-- * Income comes from the SCD2 borrower version valid when the loan was taken, so a borrower
--   whose income rose later is not counted in the wrong band.
-- * lending_era keeps pre-2009 and post-2009 loans apart: default rates were far higher before
--   Prosper's 2009 relaunch, so mixing eras would distort any segment comparison.
-- Rows are at a fine grain on purpose; dashboards and the notebook roll them up as needed.
CREATE OR REPLACE TABLE kpi.kpi_borrower_segments AS
WITH loan_sequence AS (
    SELECT
        f.*,
        b.income_range,
        row_number() OVER (
            PARTITION BY b.member_key
            ORDER BY f.origination_date_key, f.loan_number
        ) AS borrower_loan_number
    FROM marts.fact_loan f
    JOIN marts.dim_borrower b ON f.borrower_sk = b.borrower_sk
    WHERE f.loan_status_group <> 'Cancelled'
)
SELECT
    CASE WHEN rating_system = 'ProsperRating' THEN 'Post-2009' ELSE 'Pre-2009' END AS lending_era,
    CASE WHEN borrower_loan_number = 1 THEN 'First loan' ELSE 'Repeat loan' END AS borrower_type,
    grade_code,
    income_range,
    count(*)                                                     AS loans,
    sum(loan_amount)                                             AS amount_originated,
    round(avg(borrower_rate), 4)                                 AS avg_borrower_rate,
    sum(CASE WHEN is_matured THEN 1 ELSE 0 END)                  AS matured_loans,
    sum(CASE WHEN is_default AND is_matured THEN 1 ELSE 0 END)   AS matured_defaults,
    round(sum(CASE WHEN is_default AND is_matured THEN 1 ELSE 0 END)
          / nullif(sum(CASE WHEN is_matured THEN 1 ELSE 0 END), 0), 4) AS default_rate_matured
FROM loan_sequence
GROUP BY ALL
ORDER BY lending_era, borrower_type, grade_code, income_range;
