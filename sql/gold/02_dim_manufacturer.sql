-- dim_manufacturer: every manufacturer name seen in any source.
-- Surrogate key = hash of the natural key: stable across full rebuilds, no sequence table needed (ADR-004).
WITH names AS (
  SELECT manufacturer FROM complaints     WHERE manufacturer IS NOT NULL
  UNION SELECT manufacturer FROM recalls        WHERE manufacturer IS NOT NULL
  UNION SELECT manufacturer FROM investigations WHERE manufacturer IS NOT NULL
)
SELECT
  xxhash64(manufacturer)                                    AS manufacturer_key,
  manufacturer                                              AS manufacturer_name,
  -- normalised name for grouping legal-entity variants ("FORD MOTOR COMPANY" / "FORD MOTOR CO.")
  trim(regexp_replace(manufacturer,
       '[,.]|\\b(INC|LLC|L L C|CORP|CORPORATION|CO|COMPANY|LTD|LIMITED|USA|U S A|N A|NORTH AMERICA|OF AMERICA|GROUP|AG|GMBH)\\b', ''))
                                                            AS manufacturer_group
FROM names
UNION ALL SELECT -1, 'UNKNOWN', 'UNKNOWN'
