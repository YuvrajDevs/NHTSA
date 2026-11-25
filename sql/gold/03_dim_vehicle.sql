-- dim_vehicle: one row per make + model + model year seen in any source. Model year NULL = unknown (NHTSA 9999).
WITH v AS (
  SELECT make, model, model_year FROM complaints     WHERE make IS NOT NULL
  UNION SELECT make, model, model_year FROM recalls        WHERE make IS NOT NULL
  UNION SELECT make, model, model_year FROM investigations WHERE make IS NOT NULL
)
SELECT
  xxhash64(make, coalesce(model, ''), coalesce(model_year, -1))  AS vehicle_key,
  make,
  model,
  model_year,
  model_year IS NOT NULL                                         AS model_year_known,
  CASE WHEN model_year IS NULL THEN 'Unknown'
       ELSE concat(CAST(floor(model_year / 10) * 10 AS STRING), 's') END AS model_decade
FROM v
UNION ALL SELECT -1, 'UNKNOWN', 'UNKNOWN', NULL, false, 'Unknown'
