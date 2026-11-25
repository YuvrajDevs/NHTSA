"""Contract engine catches violations; PII scrubbing replaces personal details."""
from pyspark.sql import functions as F

from vsi import quality
from vsi.ai import scrub
from vsi.config import table


def test_contract_engine_flags_each_kind_of_problem(spark):
    df = spark.createDataFrame(
        [(1, "A", 5, "X1"), (1, "B", 500, "X2"), (2, None, 7, "bad id")], "id int, code string, n int, ref string")
    df.write.format("delta").mode("overwrite").save(table("gold", "qtest"))
    contract = {"table": "gold.qtest", "primary_key": ["id"], "row_count": {"min": 5},
                "columns": {"code": {"type": "string", "nullable": False, "accepted_values": ["A"]},
                            "n": {"type": "int", "range": [0, 100]},
                            "ref": {"type": "string", "regex": "^X[0-9]$"}},
                "checks": [{"name": "n positive", "sql_violations": "n <= 0", "severity": "error"}]}
    res = {(r["check"], r["column"]): r["passed"] for r in quality.check_table(spark, contract, "2026-01-01")}
    assert res[("row count", None)] is False
    assert res[("not null", "code")] is False
    assert res[("accepted values", "code")] is False
    assert res[("range [0, 100]", "n")] is False
    assert res[("format (regex)", "ref")] is False
    assert res[("primary key unique", "id")] is False
    assert res[("n positive", None)] is True


def test_pii_is_scrubbed(spark):
    text = ("CALL ME AT (913) 871-9483 OR 800-573-9523, EMAIL JOHN.DOE@MAIL.COM, VIN 1FTFW1ET5DFC10312, "
            "SEE WWW.EXAMPLE.COM. 2015 MODEL WITH 45000 MILES.")
    out = spark.createDataFrame([(text,)], "t string").select(scrub(F.col("t")).alias("o")).collect()[0].o
    assert "871-9483" not in out and "573-9523" not in out and "JOHN.DOE" not in out and "1FTFW1ET5DFC10312" not in out
    assert out.count("[PHONE]") == 2 and "[EMAIL]" in out and "[VIN]" in out and "[URL]" in out
    assert "2015 MODEL WITH 45000 MILES" in out          # ordinary numbers survive
