-- Vintage (cohort) analysis: default and loss rates by origination quarter x risk band.
-- A "vintage" is all loans issued in the same quarter. Comparing vintages is how lenders see
-- whether underwriting got better or worse over time.
-- Only 36-month loans, so every vintage is measured on the same loan length.
-- is_matured = every loan in the vintage has reached its end date, so the default rate is
-- final; for younger vintages the rate is still climbing and should not be compared directly.
CREATE OR REPLACE TABLE kpi.kpi_vintage_default AS
SELECT
    d.year_quarter                                         AS vintage,
    g.risk_band,
    count(*)                                               AS loans,
    sum(f.loan_amount)                                     AS amount_originated,
    sum(CASE WHEN f.is_default THEN 1 ELSE 0 END)          AS defaulted_loans,
    round(avg(CASE WHEN f.is_default THEN 1.0 ELSE 0 END), 4) AS default_rate,
    round(sum(f.net_principal_loss) / sum(f.loan_amount), 4) AS net_loss_rate,
    bool_and(f.is_matured)                                 AS is_matured
FROM marts.fact_loan f
JOIN marts.dim_date d         ON f.origination_date_key = d.date_key
JOIN marts.dim_credit_grade g ON f.grade_code = g.grade_code
WHERE f.term_months = 36
  AND f.loan_status_group <> 'Cancelled'
GROUP BY d.year_quarter, g.risk_band
ORDER BY vintage, g.risk_band;
