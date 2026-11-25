"""Bronze: every line of every source file, exactly as received, plus where and when it came from.

Nothing is parsed or dropped here, so any later bug can be fixed by reprocessing from bronze.
Re-running the same load date replaces that date's partition (idempotent), it never appends twice.
"""
import zipfile
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

from .config import LANDING, SAMPLE_ROWS, table
from .sources import ALL
from .spark import get_spark


def extract(load_date: str, src) -> list[str]:
    """Unzip the source's zips into landing/<date>/_extracted/ (once). Returns the .txt paths."""
    folder = LANDING / load_date
    out = folder / "_extracted"
    out.mkdir(exist_ok=True)
    paths = []
    for rel in src.files:
        z = folder / Path(rel).name
        if not z.exists():
            raise FileNotFoundError(f"{z} missing: run `python -m vsi.cli download --date {load_date}` first")
        with zipfile.ZipFile(z) as zf:
            for name in zf.namelist():
                target = out / name
                if not target.exists() or target.stat().st_size != zf.getinfo(name).file_size:
                    zf.extract(name, out)
                paths.append(str(target))
    return paths


def load(load_date: str, source: str) -> int:
    spark = get_spark()
    src = ALL[source]
    paths = extract(load_date, src)
    df = (spark.read.text(paths)
          .withColumn("_source_file", F.element_at(F.split(F.col("_metadata.file_path"), "/"), -1))
          .withColumnRenamed("value", "raw_line"))
    if SAMPLE_ROWS:
        df = df.limit(SAMPLE_ROWS)
    df = (df.withColumn("raw_line", F.regexp_replace("raw_line", "\r$", ""))   # files mix CRLF and LF line endings
            .withColumn("_field_count", F.size(F.split("raw_line", "\t")))
            .withColumn("_line_hash", F.sha2("raw_line", 256))
            .withColumn("_load_date", F.lit(load_date).cast("date"))
            .withColumn("_ingested_at", F.lit(datetime.now(timezone.utc).isoformat()).cast("timestamp")))
    (df.write.format("delta").mode("overwrite")
       .option("replaceWhere", f"_load_date = '{load_date}'")          # idempotent per load date
       .partitionBy("_load_date")
       .save(table("bronze", source)))
    n = spark.read.format("delta").load(table("bronze", source)).where(F.col("_load_date") == load_date).count()
    print(f"bronze.{source}: {n:,} lines for {load_date}")
    return n
