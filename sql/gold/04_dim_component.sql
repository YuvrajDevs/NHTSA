-- dim_component: NHTSA components form a hierarchy written as a path: SYSTEM:SUBSYSTEM:PART.
-- Flattened into levels so reports can roll up (system) or drill down (full path).
WITH c AS (
  SELECT component_path FROM complaints     WHERE component_path IS NOT NULL
  UNION SELECT component_path FROM recalls        WHERE component_path IS NOT NULL
  UNION SELECT component_path FROM investigations WHERE component_path IS NOT NULL
)
SELECT
  xxhash64(component_path)                                       AS component_key,
  component_path,
  split(component_path, ':')[0]                                  AS component_system,
  CASE WHEN size(split(component_path, ':')) > 1
       THEN concat_ws(':', slice(split(component_path, ':'), 1, 2)) ELSE component_path END AS component_subsystem,
  element_at(split(component_path, ':'), -1)                     AS component_leaf,
  size(split(component_path, ':'))                               AS hierarchy_depth
FROM c
UNION ALL SELECT -1, 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', 0
