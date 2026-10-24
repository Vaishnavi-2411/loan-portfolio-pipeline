-- Date dimension: one row per calendar day covering every loan date.
-- Lets reports slice by month / quarter / year without repeating date logic.
CREATE OR REPLACE TABLE marts.dim_date AS
WITH bounds AS (
    SELECT
        date_trunc('year', min(origination_date))                     AS first_day,
        date_trunc('year', max(coalesce(closed_date, origination_date)))
            + INTERVAL 1 YEAR                                         AS end_exclusive
    FROM staging.stg_loans
),
days AS (
    SELECT CAST(range AS DATE) AS full_date
    FROM bounds, range(bounds.first_day, bounds.end_exclusive, INTERVAL 1 DAY)
)
SELECT
    CAST(strftime(full_date, '%Y%m%d') AS INTEGER)  AS date_key,
    full_date,
    year(full_date)                                 AS year,
    quarter(full_date)                              AS quarter,
    month(full_date)                                AS month,
    monthname(full_date)                            AS month_name,
    strftime(full_date, '%Y-%m')                    AS year_month,
    year(full_date) || '-Q' || quarter(full_date)   AS year_quarter,
    dayname(full_date)                              AS day_name,
    isodow(full_date) IN (6, 7)                     AS is_weekend
FROM days;
