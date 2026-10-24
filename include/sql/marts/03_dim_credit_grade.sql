-- Credit grade dimension: groups Prosper's letter grades into risk bands.
-- 'NA' is the "unknown" member so loans without a grade still join.
CREATE OR REPLACE TABLE marts.dim_credit_grade AS
SELECT *
FROM (VALUES
    ('AA', 1, 'Prime'),
    ('A',  2, 'Prime'),
    ('B',  3, 'Near-prime'),
    ('C',  4, 'Near-prime'),
    ('D',  5, 'Subprime'),
    ('E',  6, 'Subprime'),
    ('HR', 7, 'High risk'),
    ('NC', 8, 'High risk'),   -- "No Credit" grade used before 2009
    ('NA', 9, 'Unknown')
) AS t(grade_code, grade_rank, risk_band);
