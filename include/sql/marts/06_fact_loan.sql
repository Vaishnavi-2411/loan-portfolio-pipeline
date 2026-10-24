-- Fact table. Grain: ONE ROW PER LOAN.
-- Holds the numbers we add up (amounts, payments, losses) plus foreign keys to each dimension.
CREATE OR REPLACE TABLE marts.fact_loan AS
SELECT
    l.loan_key,
    l.loan_number,
    b.borrower_sk,
    CAST(strftime(l.origination_date, '%Y%m%d') AS INTEGER) AS origination_date_key,
    CAST(strftime(l.closed_date, '%Y%m%d') AS INTEGER)      AS closed_date_key,
    l.grade_code,
    l.state_code,
    l.purpose_id,

    -- loan attributes at origination
    l.rating_system,
    l.term_months,
    l.credit_score_mid,
    l.prosper_score,
    l.debt_to_income,
    l.stated_monthly_income,
    l.investor_count,

    -- measures
    l.loan_amount,
    l.borrower_rate,
    l.borrower_apr,
    l.lender_yield,
    l.monthly_payment,
    l.customer_payments,
    l.principal_repaid,
    l.interest_and_fees,
    l.gross_principal_loss,
    l.net_principal_loss,

    -- status
    l.loan_status,
    l.loan_status_group,
    l.days_delinquent,
    l.months_since_origination,
    l.loan_status_group = 'Default'                  AS is_default,
    l.loan_status_group IN ('Paid Off', 'Default')   AS is_resolved,  -- outcome is final
    -- Matured = the full term has passed by the data snapshot date, so the loan has had its
    -- whole life to default. Default rates are measured on matured loans only; otherwise
    -- young loans (which have not had time to default) make recent periods look safer.
    l.origination_date + to_months(l.term_months) <= s.snapshot_date AS is_matured
FROM staging.stg_loans AS l
CROSS JOIN (SELECT max(origination_date) AS snapshot_date FROM staging.stg_loans) AS s
-- Point-in-time join to the SCD2 borrower dimension: pick the profile version that was
-- valid when this listing was created.
LEFT JOIN marts.dim_borrower AS b
    ON  b.member_key = l.member_key
    AND l.listing_created_at >= b.valid_from
    AND l.listing_created_at <  b.valid_to;
