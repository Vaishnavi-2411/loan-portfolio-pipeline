-- Staging: one clean, typed row per loan.
-- * casts text columns to proper types
-- * renames to snake_case
-- * removes duplicates: 871 raw rows repeat a ListingKey and differ only in ProsperScore.
--   We keep the LOWEST score (the more conservative risk view), so the choice is deterministic.
CREATE OR REPLACE TABLE staging.stg_loans AS
WITH typed AS (
    SELECT
        LoanKey                                                    AS loan_key,
        ListingKey                                                 AS listing_key,
        CAST(LoanNumber AS INTEGER)                                AS loan_number,
        MemberKey                                                  AS member_key,
        CAST(left(ListingCreationDate, 19) AS TIMESTAMP)           AS listing_created_at,
        CAST(left(LoanOriginationDate, 10) AS DATE)                AS origination_date,
        CAST(left(ClosedDate, 10) AS DATE)                         AS closed_date,
        CAST(Term AS INTEGER)                                      AS term_months,
        CAST(LoanOriginalAmount AS DECIMAL(12, 2))                 AS loan_amount,
        CAST(BorrowerRate AS DOUBLE)                               AS borrower_rate,
        CAST(BorrowerAPR AS DOUBLE)                                AS borrower_apr,
        CAST(LenderYield AS DOUBLE)                                AS lender_yield,
        CAST(MonthlyLoanPayment AS DECIMAL(12, 2))                 AS monthly_payment,
        -- Prosper switched rating systems in July 2009: CreditGrade before, ProsperRating after.
        coalesce("ProsperRating (Alpha)", CreditGrade, 'NA')       AS grade_code,
        CASE WHEN "ProsperRating (Alpha)" IS NOT NULL THEN 'ProsperRating'
             WHEN CreditGrade IS NOT NULL THEN 'CreditGrade'
             ELSE 'None' END                                       AS rating_system,
        CAST(CAST(ProsperScore AS DOUBLE) AS INTEGER)              AS prosper_score,
        CAST("ListingCategory (numeric)" AS INTEGER)               AS purpose_id,
        coalesce(BorrowerState, 'XX')                              AS state_code,
        trim(coalesce(Occupation, 'Unknown'))                      AS occupation,
        coalesce(EmploymentStatus, 'Not available')                AS employment_status,
        coalesce(IncomeRange, 'Not displayed')                     AS income_range,
        CAST(IsBorrowerHomeowner AS BOOLEAN)                       AS is_homeowner,
        CAST(StatedMonthlyIncome AS DOUBLE)                        AS stated_monthly_income,
        CAST(DebtToIncomeRatio AS DOUBLE)                          AS debt_to_income,
        (CAST(CreditScoreRangeLower AS INTEGER)
            + CAST(CreditScoreRangeUpper AS INTEGER)) / 2          AS credit_score_mid,
        LoanStatus                                                 AS loan_status,
        CAST(LoanCurrentDaysDelinquent AS INTEGER)                 AS days_delinquent,
        CAST(LoanMonthsSinceOrigination AS INTEGER)                AS months_since_origination,
        CAST(LP_CustomerPayments AS DECIMAL(12, 2))                AS customer_payments,
        CAST(LP_CustomerPrincipalPayments AS DECIMAL(12, 2))       AS principal_repaid,
        CAST(LP_InterestandFees AS DECIMAL(12, 2))                 AS interest_and_fees,
        CAST(LP_GrossPrincipalLoss AS DECIMAL(12, 2))              AS gross_principal_loss,
        CAST(LP_NetPrincipalLoss AS DECIMAL(12, 2))                AS net_principal_loss,
        CAST(Investors AS INTEGER)                                 AS investor_count,
        _loaded_at
    FROM raw.loans
),
deduped AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY listing_key
            ORDER BY prosper_score ASC NULLS LAST
        ) AS rn
    FROM typed
)
SELECT
    * EXCLUDE (rn),
    -- Business-friendly status buckets used by every KPI.
    CASE
        WHEN loan_status IN ('Chargedoff', 'Defaulted')    THEN 'Default'
        WHEN loan_status LIKE 'Past Due%'                  THEN 'Delinquent'
        WHEN loan_status = 'Completed'                     THEN 'Paid Off'
        WHEN loan_status = 'Cancelled'                     THEN 'Cancelled'
        ELSE 'Current'                                     -- Current, FinalPaymentInProgress
    END AS loan_status_group
FROM deduped
WHERE rn = 1;
