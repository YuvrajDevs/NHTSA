"""Prove change detection is exact: the changes silver detected for a simulated day must equal the changes that
scripts/simulate_next_day.py actually made (same keys, same type, nothing missing, nothing extra)."""
import json

from pyspark.sql import functions as F

from .config import LANDING, table
from .spark import get_spark


def changes(load_date: str) -> bool:
    exp_path = LANDING / load_date / "_expected_changes.json"
    if not exp_path.exists():
        raise FileNotFoundError(f"{exp_path}: only simulated days have expectations")
    expected = json.loads(exp_path.read_text())
    spark = get_spark()
    ok = True
    for source, key in (("complaints", "cmplid"), ("recalls", "record_id")):
        got = (spark.read.format("delta").load(table("silver", f"{source}_changes"))
               .where(F.col("load_date") == load_date).select(F.col(key).cast("string").alias("k"), "change_type").collect())
        got_by = {t: sorted(r.k for r in got if r.change_type == t) for t in "IUD"}
        q = (spark.read.format("delta").load(table("silver", "_quarantine"))
             .where((F.col("source") == source) & (F.col("_load_date") == load_date) & (F.col("_dq_reason") != "EMPTY_LINE")).count())
        for t in "IUD":
            exp = set(expected[source][t])
            g = set(got_by[t])
            status = "PASS" if exp == g else "FAIL"
            ok &= exp == g
            print(f"{source:<10} {t}: expected {len(exp):>4}, detected {len(g):>4}, missing {len(exp - g)}, extra {len(g - exp)}  {status}")
        exp_q = expected[source]["quarantined"]
        print(f"{source:<10} quarantined (excluding blank lines): expected {exp_q}, got {q}  {'PASS' if q == exp_q else 'FAIL'}")
        ok &= q == exp_q
    if spark.read.format("delta").load(table("silver", "recalls_history")).groupBy("record_id").agg(
            F.sum(F.col("is_current").cast("int")).alias("c")).where("c > 1").count():
        print("recalls_history: a record has more than one current version  FAIL")
        ok = False
    else:
        print("recalls_history: exactly one current version per record  PASS")
    print("RESULT:", "PASS" if ok else "FAIL")
    return ok
