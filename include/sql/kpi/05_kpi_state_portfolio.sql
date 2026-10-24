-- State portfolio: exposure and default rate per state, ranked within each Census region.
-- Shows: ROW_NUMBER partitioned by region (a "top N per group" pattern).
CREATE OR REPLACE TABLE kpi.kpi_state_portfolio AS
WITH by_state AS (
    SELECT
        g.region,
        g.state_code,
        g.state_name,
        count(*)                                        AS loans,
        sum(f.loan_amount)                              AS amount_originated,
        sum(CASE WHEN f.is_matured THEN 1 ELSE 0 END)  AS matured_loans,
        round(sum(CASE WHEN f.is_default AND f.is_matured THEN 1 ELSE 0 END)
              / nullif(sum(CASE WHEN f.is_matured THEN 1 ELSE 0 END), 0), 4)
                                                        AS default_rate_matured
    FROM marts.fact_loan f
    JOIN marts.dim_geography g ON f.state_code = g.state_code
    WHERE f.loan_status_group <> 'Cancelled'
    GROUP BY g.region, g.state_code, g.state_name
)
SELECT
    *,
    round(amount_originated / sum(amount_originated) OVER (), 4)                AS share_of_amount,
    row_number() OVER (PARTITION BY region ORDER BY amount_originated DESC)     AS rank_in_region
FROM by_state
ORDER BY region, rank_in_region;
