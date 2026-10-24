-- Risk vs return by credit grade, on MATURED loans (full term has passed).
-- Why matured only: a young loan has not had time to default yet. Mixing young and old loans
-- makes default rates look lower than they really are.
-- Only the ProsperRating system (July 2009 onward), so grades mean the same thing throughout.
-- net_return_on_principal = (interest & fees collected - net principal lost) / amount lent.
CREATE OR REPLACE TABLE kpi.kpi_grade_risk_return AS
WITH by_grade AS (
    SELECT
        g.grade_code,
        g.grade_rank,
        g.risk_band,
        count(*)                                                 AS matured_loans,
        sum(f.loan_amount)                                       AS amount_lent,
        round(avg(f.borrower_rate), 4)                           AS avg_borrower_rate,
        round(avg(CASE WHEN f.is_default THEN 1.0 ELSE 0 END), 4) AS default_rate,
        round(sum(f.net_principal_loss) / sum(f.loan_amount), 4) AS net_loss_rate,
        round((sum(f.interest_and_fees) - sum(f.net_principal_loss)) / sum(f.loan_amount), 4)
                                                                 AS net_return_on_principal
    FROM marts.fact_loan f
    JOIN marts.dim_credit_grade g ON f.grade_code = g.grade_code
    WHERE f.is_matured
      AND f.rating_system = 'ProsperRating'
    GROUP BY g.grade_code, g.grade_rank, g.risk_band
)
SELECT
    *,
    rank() OVER (ORDER BY net_return_on_principal DESC) AS return_rank,
    rank() OVER (ORDER BY default_rate DESC)            AS risk_rank
FROM by_grade
ORDER BY grade_rank;
