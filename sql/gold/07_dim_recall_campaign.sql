-- dim_recall_campaign
-- GRAIN: one row per recall campaign (NHTSA campaign number).
-- "Potentially affected units" is reported per campaign but REPEATED on every vehicle/component row of the raw
-- file. Summing it from the raw rows over-counts (one campaign with 40 rows would count its units 40 times), so it
-- lives here, at the grain where it is true.
SELECT
  r.campaign_id,
  max(r.manufacturer)                                                AS manufacturer_name,
  coalesce(max(m.manufacturer_key), -1)                              AS manufacturer_key,
  max(r.recall_type)                                                 AS recall_type,
  CASE max(r.recall_type) WHEN 'V' THEN 'Vehicle' WHEN 'E' THEN 'Equipment' WHEN 'T' THEN 'Tire'
                          WHEN 'C' THEN 'Child restraint' ELSE 'Other' END AS recall_type_name,
  min(r.report_received_date)                                        AS report_received_date,
  coalesce(CAST(date_format(min(r.report_received_date), 'yyyyMMdd') AS INT), -1) AS report_date_key,
  min(r.owner_notified_date)                                         AS owner_notified_date,
  max(r.potentially_affected)                                        AS potentially_affected,
  max(r.influenced_by)                                               AS influenced_by,
  max(r.fmvss)                                                       AS fmvss,
  max_by(r.defect_summary, length(r.defect_summary))                 AS defect_summary,
  max_by(r.consequence_summary, length(r.consequence_summary))       AS consequence_summary,
  max_by(r.corrective_action, length(r.corrective_action))           AS corrective_action,
  bool_or(coalesce(r.do_not_drive, false))                           AS do_not_drive,
  bool_or(coalesce(r.park_outside, false))                           AS park_outside,
  count(DISTINCT r.make, r.model, r.model_year)                      AS vehicles_covered,
  count(DISTINCT r.component_path)                                   AS components_covered
FROM recalls r
LEFT JOIN dim_manufacturer m ON m.manufacturer_name = r.manufacturer
WHERE r.campaign_id IS NOT NULL
GROUP BY r.campaign_id
