-- Loan purpose dimension: Prosper's numeric ListingCategory -> readable name and group.
CREATE OR REPLACE TABLE marts.dim_loan_purpose AS
SELECT
    CAST(purpose_id AS INTEGER) AS purpose_id,
    purpose_name,
    purpose_group
FROM raw.loan_purpose;
