"""Gold SQL on a tiny dataset (runs last: it builds on whatever silver holds and adds recalls).

Checks the two modelling promises that matter most: the complaint fact reconciles with silver, and recall units
are counted once per campaign even though the raw file repeats them on every row.
"""

from rows import complaint, investigation, recall
from test_silver import land
from vsi import gold, silver
from vsi.config import table


def test_gold_star_schema_and_marts(spark):
    d = "2026-02-01"
    land(spark, "complaints", d, [complaint(str(i), odino=str(5000 + i // 2), ldate=f"2024{1 + i % 2:02d}15")
                                  for i in range(10)])
    # one campaign, three vehicle rows: potentially_affected (50,000) is repeated on each raw row
    land(spark, "recalls", d, [recall("1"), recall("2", model="F-250"), recall("3", year="2016")])
    land(spark, "investigations", d, [investigation("PE23001", campno="24V100000")])
    for s in ("complaints", "recalls", "investigations"):
        silver.load(d, s)
    gold.build(d)
    g = lambda n: spark.read.format("delta").load(table("gold", n))  # noqa: E731

    live = spark.read.format("delta").load(table("silver", "complaints")).where("NOT _is_deleted").count()
    assert g("fact_complaint").count() == live
    assert g("fact_complaint").where("vehicle_key = -1 OR component_key = -1").count() == 0

    camp = g("dim_recall_campaign").collect()
    assert len(camp) == 1 and camp[0].potentially_affected == 50000 and camp[0].vehicles_covered == 3
    assert g("fact_recall_vehicle").count() == 3

    ew = g("mart_recall_early_warning").collect()[0]
    assert ew.had_prior_complaints and ew.complaints_before_3y == 5   # 10 complaint rows, 2 per complaint id
    assert ew.preceded_by_investigation
    assert ew.days_first_complaint_to_recall > 0
    assert g("dim_vehicle").groupBy("vehicle_key").count().where("count > 1").count() == 0
