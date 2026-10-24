-- Monthly origination trend: volume, pricing, month-over-month and year-over-year growth.
-- Shows: CTEs, multi-table joins, LAG, rolling-average and running-total window functions.
-- The month list comes from dim_date (a "date spine"), so months with NO loans still appear
-- as 0. Prosper issued no loans Oct 2008 - Apr 2009 (SEC registration); without the spine,
-- LAG would silently compare May 2009 with Oct 2008 and call it "month over month".
CREATE OR REPLACE TABLE kpi.kpi_monthly_originations AS
WITH months AS (
    SELECT year_month, min(full_date) AS month_start
    FROM marts.dim_date
    WHERE full_date BETWEEN
          (SELECT min(d.full_date) FROM marts.fact_loan f
             JOIN marts.dim_date d ON f.origination_date_key = d.date_key)
      AND (SELECT max(d.full_date) FROM marts.fact_loan f
             JOIN marts.dim_date d ON f.origination_date_key = d.date_key)
    GROUP BY year_month
),
loans AS (
    SELECT
        d.year_month,
        count(*)                                               AS loans_originated,
        sum(f.loan_amount)                                     AS amount_originated,
        -- amount-weighted rate: big loans count more than small ones
        sum(f.borrower_rate * f.loan_amount) / sum(f.loan_amount) AS weighted_avg_rate,
        avg(CASE WHEN g.risk_band IN ('Subprime', 'High risk') THEN 1.0 ELSE 0 END)
                                                               AS share_subprime_or_high_risk
    FROM marts.fact_loan f
    JOIN marts.dim_date d         ON f.origination_date_key = d.date_key
    JOIN marts.dim_credit_grade g ON f.grade_code = g.grade_code
    WHERE f.loan_status_group <> 'Cancelled'
    GROUP BY d.year_month
),
monthly AS (
    SELECT
        m.year_month,
        m.month_start,
        coalesce(l.loans_originated, 0)    AS loans_originated,
        coalesce(l.amount_originated, 0)   AS amount_originated,
        round(l.weighted_avg_rate, 4)      AS weighted_avg_rate,
        round(l.share_subprime_or_high_risk, 4) AS share_subprime_or_high_risk
    FROM months m
    LEFT JOIN loans l ON m.year_month = l.year_month
)
SELECT
    *,
    round(amount_originated / nullif(lag(amount_originated) OVER (ORDER BY year_month), 0) - 1, 4)
        AS mom_growth,
    round(amount_originated / nullif(lag(amount_originated, 12) OVER (ORDER BY year_month), 0) - 1, 4)
        AS yoy_growth,
    round(avg(amount_originated) OVER (
        ORDER BY year_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 0)
        AS rolling_3m_avg_amount,
    sum(amount_originated) OVER (ORDER BY year_month) AS cumulative_amount
FROM monthly
ORDER BY year_month;
