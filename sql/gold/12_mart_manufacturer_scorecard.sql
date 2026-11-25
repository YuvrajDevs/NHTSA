-- mart_manufacturer_scorecard
-- GRAIN: one row per manufacturer group x calendar year (2000 on). Uses the right grain for every measure:
-- complaints from fact_complaint (distinct complaint ids), recalls and units from dim_recall_campaign,
-- investigations from fact_investigation (distinct action numbers).
WITH c AS (
  SELECT m.manufacturer_group, d.year,
         count(DISTINCT f.complaint_id) AS complaints,
         count(DISTINCT CASE WHEN f.crash_flag = 1 THEN f.complaint_id END) AS crash_complaints,
         count(DISTINCT CASE WHEN f.fire_flag  = 1 THEN f.complaint_id END) AS fire_complaints,
         sum(f.injured) AS injured, sum(f.deaths) AS deaths
  FROM fact_complaint f
  JOIN dim_manufacturer m ON m.manufacturer_key = f.manufacturer_key
  JOIN dim_date d         ON d.date_key = f.received_date_key
  WHERE d.year >= 2000
  GROUP BY 1, 2
),
r AS (
  SELECT m.manufacturer_group, year(rc.report_received_date) AS year,
         count(*) AS recall_campaigns,
         sum(rc.potentially_affected) AS units_potentially_affected,
         sum(CAST(rc.influenced_by = 'MFR' AS INT)) AS manufacturer_initiated_recalls
  FROM dim_recall_campaign rc
  JOIN dim_manufacturer m ON m.manufacturer_key = rc.manufacturer_key
  WHERE year(rc.report_received_date) >= 2000
  GROUP BY 1, 2
),
i AS (
  SELECT m.manufacturer_group, d.year, count(DISTINCT f.action_number) AS investigations_opened
  FROM fact_investigation f
  JOIN dim_manufacturer m ON m.manufacturer_key = f.manufacturer_key
  JOIN dim_date d         ON d.date_key = f.opened_date_key
  WHERE d.year >= 2000
  GROUP BY 1, 2
)
SELECT
  coalesce(c.manufacturer_group, r.manufacturer_group, i.manufacturer_group) AS manufacturer_group,
  coalesce(c.year, r.year, i.year)                                           AS year,
  coalesce(c.complaints, 0) AS complaints, coalesce(c.crash_complaints, 0) AS crash_complaints,
  coalesce(c.fire_complaints, 0) AS fire_complaints, coalesce(c.injured, 0) AS injured, coalesce(c.deaths, 0) AS deaths,
  coalesce(r.recall_campaigns, 0) AS recall_campaigns,
  coalesce(r.units_potentially_affected, 0) AS units_potentially_affected,
  coalesce(r.manufacturer_initiated_recalls, 0) AS manufacturer_initiated_recalls,
  coalesce(i.investigations_opened, 0) AS investigations_opened
FROM c
FULL OUTER JOIN r ON r.manufacturer_group = c.manufacturer_group AND r.year = c.year
FULL OUTER JOIN i ON i.manufacturer_group = coalesce(c.manufacturer_group, r.manufacturer_group)
                 AND i.year = coalesce(c.year, r.year)
