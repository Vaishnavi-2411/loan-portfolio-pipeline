-- Loan purpose segments: portfolio share, default rate and ranking.
-- Shows: share-of-total with a window SUM, RANK, and conditional aggregation.
CREATE OR REPLACE TABLE kpi.kpi_purpose_segments AS
WITH seg AS (
    SELECT
        p.purpose_name,
        p.purpose_group,
        count(*)                                     AS loans,
        sum(f.loan_amount)                           AS amount_originated,
        sum(CASE WHEN f.is_matured THEN 1 ELSE 0 END) AS matured_loans,
        sum(CASE WHEN f.is_default AND f.is_matured THEN 1 ELSE 0 END) AS matured_defaults,
        round(avg(f.borrower_rate), 4)               AS avg_borrower_rate
    FROM marts.fact_loan f
    JOIN marts.dim_loan_purpose p ON f.purpose_id = p.purpose_id
    WHERE f.loan_status_group <> 'Cancelled'
    GROUP BY p.purpose_name, p.purpose_group
)
SELECT
    purpose_name,
    purpose_group,
    loans,
    amount_originated,
    round(amount_originated / sum(amount_originated) OVER (), 4) AS share_of_amount,
    avg_borrower_rate,
    matured_loans,
    round(matured_defaults / nullif(matured_loans, 0), 4)        AS default_rate_matured,
    rank() OVER (ORDER BY amount_originated DESC)                AS amount_rank
FROM seg
ORDER BY amount_rank;
