"""
### Weekly Delta maintenance
OPTIMIZE (small files compact) + Z-ORDER + VACUUM (7 din purani files).
KYUN alag DAG: har pipeline run ke baad OPTIMIZE = bekaar compute; hafte me ek baar kaafi.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

PIPELINE_ENV = os.environ.get("PIPELINE_ENV", "local")
CLI = f"{os.environ.get('PIPELINE_BIN', 'insurance-pipeline')} --env {PIPELINE_ENV}"

with DAG(
    dag_id="insurance_maintenance",
    doc_md=__doc__,
    schedule="40 3 * * 0",  # Sunday 03:40 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-engineering", "retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["insurance", "maintenance", PIPELINE_ENV],
) as dag:
    if PIPELINE_ENV == "databricks":
        from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

        DatabricksRunNowOperator(
            task_id="optimize_vacuum_on_databricks",
            databricks_conn_id="databricks_default",
            job_name="insurance-maintenance",
        )
    else:
        BashOperator(task_id="optimize_vacuum", bash_command=f"{CLI} maintenance")
