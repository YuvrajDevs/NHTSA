-- fact_investigation
-- GRAIN: one row per investigation x make/model/year x component x resulting recall campaign x manufacturer.
-- Investigation types form a lifecycle: PE (preliminary evaluation) -> EA (engineering analysis) -> recall.
SELECT
  i.action_number,
  i.action_type,
  CASE i.action_type WHEN 'PE' THEN 'Preliminary evaluation' WHEN 'EA' THEN 'Engineering analysis'
                     WHEN 'DP' THEN 'Defect petition' WHEN 'RQ' THEN 'Recall query' WHEN 'AQ' THEN 'Audit query'
                     WHEN 'SQ' THEN 'Service query' WHEN 'EQ' THEN 'Equipment query' ELSE 'Other' END AS action_type_name,
  coalesce(v.vehicle_key, -1)                                        AS vehicle_key,
  coalesce(cp.component_key, -1)                                     AS component_key,
  coalesce(m.manufacturer_key, -1)                                   AS manufacturer_key,
  coalesce(CAST(date_format(i.opened_date, 'yyyyMMdd') AS INT), -1)   AS opened_date_key,
  coalesce(CAST(date_format(i.closed_date, 'yyyyMMdd') AS INT), -1)   AS closed_date_key,
  i.campaign_id,
  i.campaign_id IS NOT NULL                                          AS led_to_recall,
  CASE WHEN i.closed_date >= i.opened_date THEN datediff(i.closed_date, i.opened_date) END AS days_open,
  i.subject
FROM investigations i
LEFT JOIN dim_vehicle v
       ON v.make = i.make AND v.model <=> i.model AND v.model_year <=> i.model_year AND v.vehicle_key <> -1
LEFT JOIN dim_component cp   ON cp.component_path = i.component_path
LEFT JOIN dim_manufacturer m ON m.manufacturer_name = i.manufacturer
