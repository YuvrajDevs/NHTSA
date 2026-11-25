-- fact_recall_vehicle
-- GRAIN: one row per recall record = campaign x make/model/year x component (NHTSA RECORD_ID).
-- No unit counts here (see dim_recall_campaign); count rows or distinct campaigns.
SELECT
  r.record_id                                                        AS recall_record_id,
  r.campaign_id,
  coalesce(v.vehicle_key, -1)                                        AS vehicle_key,
  coalesce(cp.component_key, -1)                                     AS component_key,
  coalesce(m.manufacturer_key, -1)                                   AS manufacturer_key,
  coalesce(CAST(date_format(r.report_received_date, 'yyyyMMdd') AS INT), -1) AS report_date_key,
  r.recall_type,
  r.mfg_begin_date,
  r.mfg_end_date
FROM recalls r
LEFT JOIN dim_vehicle v
       ON v.make = r.make AND v.model <=> r.model AND v.model_year <=> r.model_year AND v.vehicle_key <> -1
LEFT JOIN dim_component cp   ON cp.component_path = r.component_path
LEFT JOIN dim_manufacturer m ON m.manufacturer_name = r.manufacturer
