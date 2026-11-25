"""SparkSession with Delta Lake (the delta jars are downloaded from Maven Central on first run)."""
import os
import sys

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from .config import SHUFFLE_PARTITIONS, SPARK_DRIVER_MEMORY, SPARK_MASTER

_spark = None


def get_spark(app: str = "vehicle-safety-intelligence") -> SparkSession:
    global _spark
    if _spark is not None:
        return _spark
    # Python workers must use the same interpreter as the driver (otherwise pyarrow/pandas can be missing there)
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    builder = (SparkSession.builder.appName(app).master(SPARK_MASTER)
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
               .config("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS))
               .config("spark.sql.adaptive.enabled", "true")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
               .config("spark.databricks.delta.schema.autoMerge.enabled", "false")   # schema changes are explicit
               .config("spark.ui.showConsoleProgress", "false"))
    _spark = configure_spark_with_delta_pip(builder).getOrCreate()
    _spark.sparkContext.setLogLevel("WARN")
    return _spark
