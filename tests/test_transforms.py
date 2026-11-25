"""Cleaning rules (Spark expressions) on tiny DataFrames."""
from pyspark.sql import functions as F

from vsi import transforms as T


def one(spark, expr, value):
    return spark.createDataFrame([(value,)], "v string").select(expr(F.col("v")).alias("o")).collect()[0].o


def test_component_path_is_canonical(spark):
    assert one(spark, T.component_path, " steering:  steering wheel/handle bar ") == "STEERING:STEERING WHEEL/HANDLE BAR"
    assert one(spark, T.component_path, "COMMUNICATION: AUTO CRASH NOTIFICATION") == "COMMUNICATION:AUTO CRASH NOTIFICATION"


def test_component_system_is_top_level(spark):
    assert one(spark, lambda c: T.component_system(T.component_path(c)), "FUEL SYSTEM, GASOLINE:DELIVERY:FUEL PUMP") == "FUEL SYSTEM, GASOLINE"


def test_dates(spark):
    assert str(one(spark, T.to_date, "20240115")) == "2024-01-15"
    for bad in ("0", "", "00000000", "20241399", "18010101"):
        assert one(spark, T.to_date, bad) is None, bad


def test_model_year_unknown_is_null(spark):
    assert one(spark, T.model_year, "9999") is None
    assert one(spark, T.model_year, "2015") == 2015


def test_zero_means_unknown_for_counts(spark):
    assert one(spark, T.positive_or_null, "0") is None
    assert one(spark, T.positive_or_null, "125000") == 125000


def test_state_must_be_real(spark):
    assert one(spark, T.us_state, "tx") == "TX"
    assert one(spark, T.us_state, "XX") is None


def test_html_is_stripped(spark):
    assert one(spark, T.strip_html, "<p>Brake&nbsp;line <b>may</b> leak &amp; fail</p>") == "Brake line may leak & fail"


def test_row_hash_changes_with_any_column(spark):
    df = spark.createDataFrame([("a", None), ("a", "x"), ("a", None)], "k string, v string")
    h = [r.h for r in df.select(T.row_hash(["k", "v"]).alias("h")).collect()]
    assert h[0] != h[1] and h[0] == h[2]
