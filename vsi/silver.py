"""Silver: typed, standardised, de-duplicated current state of each source, built incrementally.

NHTSA publishes a FULL snapshot every day, not a list of changes. Each load therefore:
  1. parses that day's bronze lines (bad lines -> quarantine, with a reason)
  2. compares the snapshot with the current silver table on the business key, using a hash of all columns
     -> I (new), U (changed), D (no longer published), unchanged
  3. writes the change set to <source>_changes (an audit log of what changed when)
  4. MERGEs the changes into silver.<source> (inserts, updates, soft deletes)
  5. for recalls, also keeps full version history (SCD type 2) in silver.recalls_history
  6. records the load in silver._load_log; a load date that was already processed is skipped (idempotent), and an
     older date than the last one processed is refused (no accidental time travel)
"""
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

import time

from .config import DELETION_VECTORS, table
from .spark import get_spark
from .transforms import KEYS, MIN_FIELDS, TYPED, row_hash
from .sources import ALL

META = ["_row_hash", "_first_load_date", "_updated_load_date", "_is_deleted", "_deleted_load_date"]


def semi_join(df: DataFrame, keys: DataFrame, key: list[str]) -> DataFrame:
    """Rows of df whose key appears in keys, treating NULL as equal to NULL.

    A plain df.join(keys, key, "left_semi") uses `=`, and NULL = NULL is not true in SQL, so every row with a NULL
    in any key column would be silently dropped (build-log F5: 35,423 investigations lost this way).
    """
    k = keys.select(*[F.col(c).alias(f"_k_{c}") for c in key])
    cond = [F.col(c).eqNullSafe(F.col(f"_k_{c}")) for c in key]
    return df.join(k, cond, "left_semi")


# ------------------------------------------------------------------ 1. parse
def parse(bronze: DataFrame, source: str) -> tuple[DataFrame, DataFrame]:
    """Bronze lines -> (typed rows, quarantined rows with a reason)."""
    n_cols = len(ALL[source].columns)
    df = bronze.withColumn("f", F.split("raw_line", "\t", -1))
    reason = (F.when(F.trim("raw_line") == "", "EMPTY_LINE")
               .when(F.size("f") > n_cols, "TOO_MANY_FIELDS")
               .when(F.size("f") < MIN_FIELDS[source], "TOO_FEW_FIELDS"))
    df = df.withColumn("_dq_reason", reason)
    typed = df.where("_dq_reason IS NULL").select(*TYPED[source](), "_load_date", "_source_file", "_line_hash")
    key = KEYS[source]
    # a row is unusable without its business key
    missing_key = F.lit(False)
    for k in key[:1]:
        missing_key = missing_key | F.col(k).isNull()
    typed = typed.withColumn("_dq_reason", F.when(missing_key, "MISSING_KEY"))
    # duplicates of the business key inside one snapshot: keep one, quarantine the rest
    w = Window.partitionBy(*key).orderBy("_source_file", "_line_hash")
    typed = typed.withColumn("_rn", F.row_number().over(w))
    typed = typed.withColumn("_dq_reason", F.coalesce("_dq_reason", F.when(F.col("_rn") > 1, "DUPLICATE_KEY"))).drop("_rn")

    bad_lines = df.where("_dq_reason IS NOT NULL").select("_dq_reason", "raw_line", "_source_file", "_load_date")
    bad_typed = (typed.where("_dq_reason IS NOT NULL")
                 .select("_dq_reason", F.to_json(F.struct(*[c for c in typed.columns if not c.startswith("_")])).alias("raw_line"),
                         "_source_file", "_load_date"))
    good = typed.where("_dq_reason IS NULL").drop("_dq_reason", "_source_file", "_line_hash")
    quarantine = bad_lines.unionByName(bad_typed).withColumn("source", F.lit(source))
    return good, quarantine


# ------------------------------------------------------------------ load log (control table)
def _log_path():
    return table("silver", "_load_log")


def processed_dates(source: str) -> list[str]:
    spark = get_spark()
    if not DeltaTable.isDeltaTable(spark, _log_path()):
        return []
    rows = spark.read.format("delta").load(_log_path()).where(F.col("source") == source).select("load_date").collect()
    return sorted(str(r.load_date) for r in rows)


def _record(source: str, load_date: str, stats: dict):
    spark = get_spark()
    row = {"source": source, "load_date": load_date, "finished_at": datetime.now(timezone.utc).isoformat(), **stats}
    df = spark.createDataFrame([row]).select(
        "source", F.col("load_date").cast("date"), F.col("finished_at").cast("timestamp"),
        *[F.col(k).cast("long") for k in stats])
    df.write.format("delta").mode("append").option("mergeSchema", "true").save(_log_path())


# ------------------------------------------------------------------ 2-4. change detection + merge
def load(load_date: str, source: str, force: bool = False) -> dict:
    spark = get_spark()
    t_start = time.time()
    done = processed_dates(source)
    if load_date in done and not force:
        print(f"silver.{source}: {load_date} already processed, skipping (idempotent re-run)")
        return {"skipped": 1}
    if done and load_date < done[-1] and not force:
        raise ValueError(f"silver.{source}: {load_date} is older than the last processed load {done[-1]}. "
                         f"Snapshots must be applied in order; use `rebuild` to replay history.")

    bronze = spark.read.format("delta").load(table("bronze", source)).where(F.col("_load_date") == load_date)
    good, quarantine = parse(bronze, source)
    (quarantine.write.format("delta").mode("overwrite")
        .option("replaceWhere", f"source = '{source}' AND _load_date = '{load_date}'")
        .partitionBy("source", "_load_date").save(table("silver", "_quarantine")))

    key = KEYS[source]
    business = [c for c in good.columns if c != "_load_date"]
    snap = (good.withColumn("_row_hash", row_hash(business))
                .withColumn("_load_date", F.lit(load_date).cast("date"))
                .localCheckpoint(eager=True))          # computed once, reused below (no re-parse of bronze)
    target_path = table("silver", source)

    if not DeltaTable.isDeltaTable(spark, target_path):   # first load: compare against an empty table
        current = snap.select(*key, "_row_hash").limit(0).withColumn("_is_deleted", F.lit(False))
    else:
        current = spark.read.format("delta").load(target_path).select(*key, "_row_hash", "_is_deleted")

    s, t = snap.alias("s"), current.alias("t")
    cond = [F.col(f"s.{k}").eqNullSafe(F.col(f"t.{k}")) for k in key]
    joined = s.join(t, cond, "full_outer")
    in_s = F.col("s._row_hash").isNotNull()
    in_t = F.col("t._row_hash").isNotNull()
    change = (F.when(in_s & (~in_t | F.col("t._is_deleted")), "I")
               .when(in_s & in_t & (F.col("s._row_hash") != F.col("t._row_hash")), "U")
               .when(~in_s & in_t & ~F.col("t._is_deleted"), "D"))
    changes = (joined.withColumn("change_type", change).where("change_type IS NOT NULL")
               .select(*[F.coalesce(F.col(f"s.{k}"), F.col(f"t.{k}")).alias(k) for k in key], "change_type",
                       F.col("t._row_hash").alias("old_hash"), F.col("s._row_hash").alias("new_hash"))
               .withColumn("load_date", F.lit(load_date).cast("date"))
               .localCheckpoint(eager=True))
    (changes.write.format("delta").mode("overwrite").option("replaceWhere", f"load_date = '{load_date}'")
        .partitionBy("load_date").save(table("silver", f"{source}_changes")))
    counts = {r.change_type: r["count"] for r in changes.groupBy("change_type").count().collect()}

    upserts = (semi_join(snap, changes.where("change_type IN ('I','U')"), key)
                   .withColumn("_first_load_date", F.lit(load_date).cast("date"))
                   .withColumn("_updated_load_date", F.lit(load_date).cast("date"))
                   .withColumn("_is_deleted", F.lit(False))
                   .withColumn("_deleted_load_date", F.lit(None).cast("date"))
                   .drop("_load_date"))
    if not DeltaTable.isDeltaTable(spark, target_path):
        upserts.write.format("delta").save(target_path)
        _set_deletion_vectors(spark, target_path)
    else:
        # ONE merge for inserts, updates AND soft deletes. Every MERGE rewrites each data file that holds a matched
        # row; doing deletes in a second MERGE rewrote the whole 900 MB table twice (docs/performance.md).
        on = " AND ".join(f"t.{k} <=> s.{k}" for k in key)
        deleted = (changes.where("change_type = 'D'").select(*key)
                   .withColumn("_op", F.lit("delete")))
        source_df = upserts.withColumn("_op", F.lit("upsert")).unionByName(deleted, allowMissingColumns=True)
        cols = [c for c in upserts.columns]
        (DeltaTable.forPath(spark, target_path).alias("t")
            .merge(source_df.alias("s"), on)
            .whenMatchedUpdate(condition="s._op = 'upsert'",                       # changed or re-appeared records
                               set={c: f"s.{c}" for c in cols if c != "_first_load_date"})
            .whenMatchedUpdate(condition="s._op = 'delete'",                       # no longer published: soft delete
                               set={"_is_deleted": "true", "_deleted_load_date": f"DATE'{load_date}'"})
            .whenNotMatchedInsert(condition="s._op = 'upsert'", values={c: f"s.{c}" for c in cols})
            .execute())

    if source == "recalls":
        scd2(snap, changes, load_date)

    if DeltaTable.isDeltaTable(spark, target_path):      # guard: silver must hold every non-deleted snapshot row
        live = spark.read.format("delta").load(target_path).where("NOT _is_deleted").count()
        if live != snap.count():
            raise AssertionError(f"silver.{source}: {live:,} live rows but the snapshot has {snap.count():,}")
    stats = {"rows_in_snapshot": snap.count(), "quarantined": quarantine.count(),
             "inserted": counts.get("I", 0), "updated": counts.get("U", 0), "deleted": counts.get("D", 0),
             "seconds": round(time.time() - t_start)}
    _record(source, load_date, stats)
    print(f"silver.{source} {load_date}: " + ", ".join(f"{k}={v:,}" for k, v in stats.items()))
    return stats


def _set_deletion_vectors(spark, path):
    """With deletion vectors, a MERGE that changes 750 of 2.3M rows marks those rows as removed instead of rewriting
    every data file that contains one of them (docs/performance.md)."""
    flag = "true" if DELETION_VECTORS else "false"
    spark.sql(f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES ('delta.enableDeletionVectors' = '{flag}')")


# ------------------------------------------------------------------ 5. SCD type 2 history for recalls
def scd2(snap: DataFrame, changes: DataFrame, load_date: str):
    """silver.recalls_history: one row per version of each recall record, with valid_from / valid_to / is_current.

    Classic two-step SCD2 MERGE: close the current version of every changed or deleted key, then insert the new
    version of every new or changed key. Running it twice for the same load_date is prevented by the load log.
    """
    spark = get_spark()
    path = table("silver", "recalls_history")
    key = KEYS["recalls"]
    new_versions = (semi_join(snap, changes.where("change_type IN ('I','U')"), key)
                        .drop("_load_date")
                        .withColumn("valid_from", F.lit(load_date).cast("date"))
                        .withColumn("valid_to", F.lit(None).cast("date"))
                        .withColumn("is_current", F.lit(True)))
    if not DeltaTable.isDeltaTable(spark, path):
        new_versions.write.format("delta").save(path)
        _set_deletion_vectors(spark, path)
        return
    closing = changes.where("change_type IN ('U','D')").select(*key)
    on = " AND ".join(f"t.{k} <=> s.{k}" for k in key) + " AND t.is_current"
    (DeltaTable.forPath(spark, path).alias("t").merge(closing.alias("s"), on)
        .whenMatchedUpdate(set={"valid_to": f"DATE'{load_date}'", "is_current": "false"})
        .execute())
    new_versions.write.format("delta").mode("append").save(path)


# ------------------------------------------------------------------ rebuild (replay history from bronze)
def rebuild(source: str):
    """Drop silver for one source and replay every bronze load date in order. Used after fixing a parsing bug."""
    import shutil
    spark = get_spark()
    for name in (source, f"{source}_changes", f"{source}_history" if source == "recalls" else None):
        if name:
            shutil.rmtree(table("silver", name), ignore_errors=True)
    if DeltaTable.isDeltaTable(spark, _log_path()):
        DeltaTable.forPath(spark, _log_path()).delete(F.col("source") == source)
    if DeltaTable.isDeltaTable(spark, table("silver", "_quarantine")):
        DeltaTable.forPath(spark, table("silver", "_quarantine")).delete(F.col("source") == source)
    dates = sorted(str(r[0]) for r in spark.read.format("delta").load(table("bronze", source))
                   .select("_load_date").distinct().collect())
    print(f"rebuilding silver.{source} from bronze: {dates}")
    for d in dates:
        load(d, source)
