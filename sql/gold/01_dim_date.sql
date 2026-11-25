-- dim_date: one row per calendar day, 1949-2030. Role-playing: used as fail date, received date, recall date...
-- date_key is an integer YYYYMMDD (readable and sortable); -1 = unknown date.
SELECT
  CAST(date_format(d, 'yyyyMMdd') AS INT)        AS date_key,
  d                                              AS full_date,
  year(d)                                        AS year,
  quarter(d)                                     AS quarter,
  month(d)                                       AS month,
  date_format(d, 'MMMM')                         AS month_name,
  date_format(d, 'yyyy-MM')                      AS year_month,
  CAST(date_trunc('MONTH', d) AS DATE)           AS month_start,
  weekofyear(d)                                  AS iso_week,
  dayofweek(d)                                   AS day_of_week,
  dayofweek(d) IN (1, 7)                         AS is_weekend
FROM (SELECT explode(sequence(DATE'1949-01-01', DATE'2030-12-31', INTERVAL 1 DAY)) AS d)
UNION ALL
SELECT -1, NULL, NULL, NULL, NULL, 'Unknown', 'Unknown', NULL, NULL, NULL, NULL
