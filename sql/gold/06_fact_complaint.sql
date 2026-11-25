-- fact_complaint
-- GRAIN: one row per complaint x component (NHTSA CMPLID). One complaint (odino) can name several components,
-- so COUNT(DISTINCT odino) counts complaints and COUNT(*) counts complaint-component pairs. Both are exposed
-- as named metrics in metrics.yml so nobody has to remember this.
SELECT
  c.cmplid                                                          AS complaint_component_id,
  c.odino                                                           AS complaint_id,          -- degenerate dimension
  coalesce(v.vehicle_key, -1)                                       AS vehicle_key,
  coalesce(cp.component_key, -1)                                    AS component_key,
  coalesce(m.manufacturer_key, -1)                                  AS manufacturer_key,
  coalesce(s.state_key, -1)                                         AS state_key,
  coalesce(CAST(date_format(c.fail_date, 'yyyyMMdd') AS INT), -1)     AS fail_date_key,
  coalesce(CAST(date_format(c.received_date, 'yyyyMMdd') AS INT), -1) AS received_date_key,
  c.complaint_type,
  CAST(coalesce(c.crash, false) AS INT)                             AS crash_flag,
  CAST(coalesce(c.fire, false) AS INT)                              AS fire_flag,
  c.injured                                                         AS injured,
  c.deaths                                                          AS deaths,
  c.miles                                                           AS miles_at_failure,
  CASE WHEN c.fail_date IS NOT NULL AND c.received_date >= c.fail_date
       THEN datediff(c.received_date, c.fail_date) END              AS days_fail_to_report,
  c.police_report,
  c.medical_attention,
  c.vehicle_towed
FROM complaints c
LEFT JOIN dim_vehicle v
       ON v.make = c.make AND v.model <=> c.model AND v.model_year <=> c.model_year AND v.vehicle_key <> -1
LEFT JOIN dim_component cp    ON cp.component_path = c.component_path
LEFT JOIN dim_manufacturer m  ON m.manufacturer_name = c.manufacturer
LEFT JOIN dim_state s         ON s.state_code = c.state
