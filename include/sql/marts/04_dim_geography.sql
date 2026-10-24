-- Geography dimension: US state -> Census region/division (from the state_region seed).
CREATE OR REPLACE TABLE marts.dim_geography AS
SELECT state_code, state_name, region, division
FROM raw.state_region
UNION ALL
SELECT 'XX', 'Unknown', 'Unknown', 'Unknown';  -- loans with no state on file
