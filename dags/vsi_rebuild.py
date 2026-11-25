"""Manual DAG: replay silver for one source from bronze, then rebuild gold and re-check quality.

Use after fixing a parsing or cleaning bug (build-log F4 and F5 were fixed exactly this way). Trigger with
config {"source": "investigations"}. Bronze keeps every line ever received, so nothing needs re-downloading.
"""
from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

CLI = "cd /opt/vsi && python -m vsi.cli"

with DAG(
    dag_id="vsi_rebuild",
    schedule=None,
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    params={"source": Param("investigations", enum=["complaints", "recalls", "investigations"])},
    tags=["nhtsa", "maintenance"],
    doc_md=__doc__,
) as dag:
    rebuild = BashOperator(task_id="rebuild_silver", pool="spark", bash_command=f"{CLI} rebuild {{{{ params.source }}}}")
    gold = BashOperator(task_id="gold", pool="spark", bash_command=f"{CLI} gold --date {{{{ ds }}}}")
    quality = BashOperator(task_id="quality_gate", pool="spark", retries=0, bash_command=f"{CLI} quality --date {{{{ ds }}}}")
    rebuild >> gold >> quality
