-- mart_component_risk_monthly  (data product: "which components are trending toward a recall?")
-- GRAIN: one row per make x component system x calendar month, for every month from the pair's first complaint to
-- the latest month in the data, INCLUDING months with zero complaints (otherwise the baseline would ignore quiet
-- months and be biased upward).
-- SPIKE RULE (documented in metrics.yml): at least 10 complaints in the month AND at least 3x the average of the
-- previous 12 months (zeros included), with at least 6 months of history.
WITH monthly AS (
  SELECT c.make, c.component_system,
         CAST(date_trunc('MONTH', c.received_date) AS DATE)          AS month_start,
         count(DISTINCT c.odino)                                     AS complaints,
         count(DISTINCT CASE WHEN c.crash THEN c.odino END)          AS crash_complaints,
         count(DISTINCT CASE WHEN c.fire  THEN c.odino END)          AS fire_complaints,
         sum(c.injured)                                              AS injured,
         sum(c.deaths)                                               AS deaths
  FROM complaints c
  WHERE c.make IS NOT NULL AND c.component_system IS NOT NULL AND c.received_date IS NOT NULL
  GROUP BY 1, 2, 3
),
pairs AS (   -- only pairs with enough history to say anything (keeps the dense grid small)
  SELECT make, component_system, min(month_start) AS first_month
  FROM monthly GROUP BY 1, 2 HAVING sum(complaints) >= 30
),
bounds AS (SELECT max(month_start) AS last_month FROM monthly),
grid AS (
  SELECT p.make, p.component_system,
         explode(sequence(p.first_month, b.last_month, INTERVAL 1 MONTH)) AS month_start
  FROM pairs p CROSS JOIN bounds b
),
filled AS (
  SELECT g.make, g.component_system, g.month_start,
         coalesce(m.complaints, 0) AS complaints, coalesce(m.crash_complaints, 0) AS crash_complaints,
         coalesce(m.fire_complaints, 0) AS fire_complaints, coalesce(m.injured, 0) AS injured,
         coalesce(m.deaths, 0) AS deaths
  FROM grid g LEFT JOIN monthly m
    ON m.make = g.make AND m.component_system = g.component_system AND m.month_start = g.month_start
),
scored AS (
  SELECT *,
    avg(complaints) OVER w12  AS baseline_avg_12m,
    count(*)        OVER w12  AS history_months,
    sum(complaints) OVER (PARTITION BY make, component_system ORDER BY month_start
                          ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS complaints_rolling_3m
  FROM filled
  WINDOW w12 AS (PARTITION BY make, component_system ORDER BY month_start ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING)
)
SELECT
  make, component_system, month_start,
  CAST(date_format(month_start, 'yyyyMMdd') AS INT)                  AS month_date_key,
  complaints, crash_complaints, fire_complaints, injured, deaths, complaints_rolling_3m,
  round(baseline_avg_12m, 2)                                         AS baseline_avg_12m,
  round(try_divide(complaints, nullif(baseline_avg_12m, 0)), 2)      AS spike_ratio,
  (complaints >= 10 AND history_months >= 6
   AND complaints >= 3 * coalesce(baseline_avg_12m, 0))              AS is_spike
FROM scored
