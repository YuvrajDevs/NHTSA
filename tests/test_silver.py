"""Silver end to end on tiny snapshots: parsing, quarantine, change detection, idempotency, ordering."""
from datetime import datetime

import pytest
from pyspark.sql import functions as F

from rows import complaint, investigation
from vsi import silver
from vsi.config import table


def land(spark, source, load_date, lines):
    """Write raw lines into bronze the way bronze.load would."""
    df = (spark.createDataFrame([(ln,) for ln in lines], "raw_line string")
          .withColumn("_source_file", F.lit("test.txt"))
          .withColumn("_field_count", F.size(F.split("raw_line", "\t")))
          .withColumn("_line_hash", F.sha2("raw_line", 256))
          .withColumn("_load_date", F.lit(load_date).cast("date"))
          .withColumn("_ingested_at", F.lit(datetime.now()).cast("timestamp")))
    (df.write.format("delta").mode("overwrite").option("replaceWhere", f"_load_date = '{load_date}'")
       .partitionBy("_load_date").save(table("bronze", source)))


def changes(spark, source, d):
    rows = spark.read.format("delta").load(table("silver", f"{source}_changes")).where(F.col("load_date") == d).collect()
    return {t: sorted(r.cmplid for r in rows if r.change_type == t) for t in "IUD"}


def test_complaints_two_days(spark):
    land(spark, "complaints", "2026-01-01", [complaint("1"), complaint("2"), complaint("3"), "", "BROKEN\tLINE"])
    s1 = silver.load("2026-01-01", "complaints")
    assert (s1["inserted"], s1["quarantined"]) == (3, 2)          # blank line + too few fields

    # day 2: 1 changed (narrative), 2 deleted, 4 new, 1 unchanged
    land(spark, "complaints", "2026-01-02", [complaint("1", text="BRAKES FAILED AGAIN"), complaint("3"), complaint("4")])
    s2 = silver.load("2026-01-02", "complaints")
    assert (s2["inserted"], s2["updated"], s2["deleted"]) == (1, 1, 1)
    assert changes(spark, "complaints", "2026-01-02") == {"I": ["4"], "U": ["1"], "D": ["2"]}
    cur = {r.cmplid: r for r in spark.read.format("delta").load(table("silver", "complaints")).collect()}
    assert cur["2"]._is_deleted and str(cur["2"]._deleted_load_date) == "2026-01-02"
    assert cur["1"].narrative == "BRAKES FAILED AGAIN" and str(cur["1"]._first_load_date) == "2026-01-01"


def test_rerun_is_skipped_and_old_dates_refused(spark):
    assert silver.load("2026-01-02", "complaints") == {"skipped": 1}
    with pytest.raises(ValueError, match="older than the last processed"):
        silver.load("2025-12-31", "complaints")


def test_null_key_parts_are_not_lost(spark):
    """Regression test for build-log F5: rows whose key contains NULL (unknown model year) were silently dropped."""
    lines = [investigation("PE20001", year="9999"), investigation("PE20001", year="2015"),
             investigation("PE20002", year="9999", campno="20V100000")]
    land(spark, "investigations", "2026-01-01", lines)
    assert silver.load("2026-01-01", "investigations")["inserted"] == 3
    land(spark, "investigations", "2026-01-02", lines)
    s = silver.load("2026-01-02", "investigations")
    assert (s["inserted"], s["updated"], s["deleted"]) == (0, 0, 0)
    assert spark.read.format("delta").load(table("silver", "investigations")).count() == 3


def test_duplicate_keys_are_quarantined(spark):
    land(spark, "complaints", "2026-01-03", [complaint("1", text="BRAKES FAILED AGAIN"), complaint("3"), complaint("4"),
                                             complaint("4", text="SAME ID TWICE")])
    s = silver.load("2026-01-03", "complaints")
    assert s["quarantined"] == 1
    q = spark.read.format("delta").load(table("silver", "_quarantine")).where("_load_date = '2026-01-03'")
    assert [r._dq_reason for r in q.collect()] == ["DUPLICATE_KEY"]
