"""Gold: run sql/gold/*.sql in file-name order. Each file is one SELECT; its result becomes gold.<name>.

Silver tables are exposed to SQL as views holding only live (not deleted) rows: complaints, recalls,
investigations. Seeds (small reference CSVs) are exposed as seed_<name>. Every gold table is also registered as a
view, so later files can build on earlier ones (the file order is the dependency order).

Gold tables are rebuilt in full each run (overwrite): they are derived, small enough, and a full rebuild is the
simplest way to guarantee they always match silver.
"""
import re
import time

from pyspark.sql import functions as F

from .config import SQL_DIR, table
from .spark import get_spark


def register_inputs(spark):
    for name in ("complaints", "recalls", "investigations"):
        (spark.read.format("delta").load(table("silver", name)).where("NOT _is_deleted")
         .createOrReplaceTempView(name))
    for csv in sorted((SQL_DIR / "seeds").glob("*.csv")):
        spark.read.option("header", True).csv(str(csv)).createOrReplaceTempView(f"seed_{csv.stem}")


def models() -> list[tuple[str, str]]:
    """[(table_name, sql)] from files like sql/gold/06_fact_complaint.sql."""
    out = []
    for f in sorted((SQL_DIR / "gold").glob("*.sql")):
        name = re.sub(r"^\d+_", "", f.stem)
        out.append((name, f.read_text()))
    return out


def build(load_date: str | None = None, only: list[str] | None = None) -> dict:
    spark = get_spark()
    register_inputs(spark)
    timings = {}
    for name, sql in models():
        path = table("gold", name)
        if only and name not in only:
            spark.read.format("delta").load(path).createOrReplaceTempView(name)
            continue
        t0 = time.time()
        df = spark.sql(sql)
        if load_date:
            df = df.withColumn("_built_for_load_date", F.lit(load_date).cast("date"))
        (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path))
        spark.read.format("delta").load(path).createOrReplaceTempView(name)
        n = spark.read.format("delta").load(path).count()
        timings[name] = round(time.time() - t0, 1)
        print(f"gold.{name}: {n:,} rows ({timings[name]} s)")
    return timings
