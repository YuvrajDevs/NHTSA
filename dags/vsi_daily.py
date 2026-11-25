"""Daily Vehicle Safety Intelligence pipeline.

download -> bronze (per source) -> silver (per source) -> gold -> quality gate -> [ai documents, catalog]

- Each task calls the same CLI you can run by hand: `python -m vsi.cli <step> --date {{ ds }}`. Airflow only
  orchestrates (order, retries, alerts, history); all logic lives in the vsi package, which is unit-tested.
- {{ ds }} is the run's LOGICAL date, so a re-run of 2026-11-25 processes 2026-11-25's snapshot, never "today".
- Spark tasks share a pool with one slot: on one machine, two Spark jobs at once would just fight for memory.
- The quality task fails on any blocking data-contract violation, so bad data never reaches the AI layer or the
  catalog; the gold tables from the failed run stay visible to engineers for debugging.
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

SOURCES = ["complaints", "recalls", "investigations"]
CLI = "cd /opt/vsi && python -m vsi.cli"

default_args = {
    "owner": "vehicle-safety-data-product",
    # production: retry transient failures (network, NHTSA hiccups); development: VSI_TASK_RETRIES=0 to fail fast
    "retries": int(os.environ.get("VSI_TASK_RETRIES", "2")),
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="vsi_daily",
    description="NHTSA complaints, recalls and investigations -> governed data product",
    schedule="30 7 * * *",                 # NHTSA refreshes files overnight (US time); 07:30 UTC is after that
    start_date=datetime(2026, 11, 1),
    catchup=False,                          # NHTSA only serves today's snapshot, so past dates can't be downloaded
    max_active_runs=1,                      # snapshots must be applied in order (silver refuses older dates)
    default_args=default_args,
    tags=["nhtsa", "data-product"],
    doc_md=__doc__,
) as dag:
    download = BashOperator(task_id="download", bash_command=f"{CLI} download --date {{{{ ds }}}}")

    with TaskGroup("bronze") as bronze:
        for s in SOURCES:
            BashOperator(task_id=s, pool="spark", bash_command=f"{CLI} bronze --date {{{{ ds }}}} --sources {s}")

    with TaskGroup("silver") as silver:
        for s in SOURCES:
            BashOperator(task_id=s, pool="spark", bash_command=f"{CLI} silver --date {{{{ ds }}}} --sources {s}")

    gold = BashOperator(task_id="gold", pool="spark", bash_command=f"{CLI} gold --date {{{{ ds }}}}")
    quality = BashOperator(task_id="quality_gate", pool="spark", retries=0,      # a data problem won't fix itself on retry
                           bash_command=f"{CLI} quality --date {{{{ ds }}}}")
    ai = BashOperator(task_id="ai_documents", pool="spark", bash_command=f"{CLI} ai --date {{{{ ds }}}}")
    catalog = BashOperator(task_id="catalog", pool="spark", bash_command=f"{CLI} catalog --date {{{{ ds }}}}")

    download >> bronze >> silver >> gold >> quality >> [ai, catalog]
