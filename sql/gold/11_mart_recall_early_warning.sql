-- mart_recall_early_warning  (data product: "could complaints have warned us before this recall?")
-- GRAIN: one row per VEHICLE recall campaign whose report was received from 2000 on (complaints start in 1995, so
-- every campaign has at least ~5 years of possible complaint history).
-- A complaint "matches" a campaign when it is about a make + model + model year + component system that the campaign
-- covers, and was received in the 3 years (1,095 days) BEFORE the recall report date.
WITH campaigns AS (
  SELECT campaign_id, manufacturer_name, report_received_date, potentially_affected, vehicles_covered, influenced_by
  FROM dim_recall_campaign
  WHERE recall_type = 'V' AND report_received_date >= DATE'2000-01-01'
),
coverage AS (   -- what each campaign covers, at the level complaints can be matched on
  SELECT DISTINCT r.campaign_id, r.make, r.model, r.model_year, r.component_system
  FROM recalls r JOIN campaigns c ON c.campaign_id = r.campaign_id
  WHERE r.make IS NOT NULL AND r.model IS NOT NULL AND r.model_year IS NOT NULL
),
complaint_keys AS (  -- one row per complaint x system (a complaint naming 3 brake parts counts once for BRAKES)
  SELECT DISTINCT odino, make, model, model_year, component_system, received_date,
         coalesce(crash, false) AS crash, coalesce(fire, false) AS fire
  FROM complaints
  WHERE make IS NOT NULL AND model IS NOT NULL AND model_year IS NOT NULL AND received_date IS NOT NULL
),
matched AS (
  SELECT cv.campaign_id, k.odino, k.received_date, k.crash, k.fire
  FROM coverage cv
  JOIN campaigns c     ON c.campaign_id = cv.campaign_id
  JOIN complaint_keys k
    ON  k.make = cv.make AND k.model = cv.model AND k.model_year = cv.model_year
    AND k.component_system = cv.component_system
    AND k.received_date <  c.report_received_date
    AND k.received_date >= date_sub(c.report_received_date, 1095)
),
per_campaign AS (
  SELECT m.campaign_id,
         count(DISTINCT m.odino)                                                     AS complaints_before_3y,
         count(DISTINCT CASE WHEN m.crash THEN m.odino END)                          AS crash_complaints_before,
         count(DISTINCT CASE WHEN m.fire  THEN m.odino END)                          AS fire_complaints_before,
         min(m.received_date)                                                        AS first_complaint_date,
         count(DISTINCT CASE WHEN m.received_date >= date_sub(c.report_received_date, 90) THEN m.odino END)
                                                                                     AS complaints_last_90d
  FROM matched m JOIN campaigns c ON c.campaign_id = m.campaign_id
  GROUP BY m.campaign_id
),
spikes AS (   -- earliest monthly spike for any (make, system) the campaign covers, within 24 months before the report
  SELECT cv.campaign_id, min(s.month_start) AS first_spike_month
  FROM (SELECT DISTINCT campaign_id, make, component_system FROM coverage) cv
  JOIN campaigns c ON c.campaign_id = cv.campaign_id
  JOIN mart_component_risk_monthly s
    ON  s.make = cv.make AND s.component_system = cv.component_system AND s.is_spike
    AND s.month_start <  c.report_received_date
    AND s.month_start >= add_months(c.report_received_date, -24)
  GROUP BY cv.campaign_id
),
investigated AS (
  SELECT DISTINCT campaign_id FROM fact_investigation WHERE campaign_id IS NOT NULL
)
SELECT
  c.campaign_id, c.manufacturer_name, c.report_received_date,
  CAST(date_format(c.report_received_date, 'yyyyMMdd') AS INT)                  AS report_date_key,
  year(c.report_received_date)                                                  AS report_year,
  c.potentially_affected, c.vehicles_covered, c.influenced_by,
  i.campaign_id IS NOT NULL                                                     AS preceded_by_investigation,
  coalesce(p.complaints_before_3y, 0)                                           AS complaints_before_3y,
  coalesce(p.crash_complaints_before, 0)                                        AS crash_complaints_before,
  coalesce(p.fire_complaints_before, 0)                                         AS fire_complaints_before,
  coalesce(p.complaints_last_90d, 0)                                            AS complaints_last_90d,
  p.first_complaint_date,
  datediff(c.report_received_date, p.first_complaint_date)                      AS days_first_complaint_to_recall,
  s.first_spike_month,
  datediff(c.report_received_date, s.first_spike_month)                         AS days_spike_to_recall,
  coalesce(p.complaints_before_3y, 0) > 0                                       AS had_prior_complaints,
  s.first_spike_month IS NOT NULL                                               AS had_spike_signal
FROM campaigns c
LEFT JOIN per_campaign p ON p.campaign_id = c.campaign_id
LEFT JOIN spikes s       ON s.campaign_id = c.campaign_id
LEFT JOIN investigated i ON i.campaign_id = c.campaign_id
