"""
### Insurance ELT pipeline (Medallion)

`PIPELINE_ENV` (.env) decide karta hai pipeline kahan chalega:

* `local` -> isi Docker container me: sample file -> Landing -> Bronze (Spark) -> Reconcile
  -> Silver (Spark) -> DQ gate -> Gold (dbt + DuckDB) -> dbt docs
* `databricks` -> Databricks Free Edition par poora job `insurance-medallion-pipeline`
  (inbox Volume -> ... -> gold dbt -> report notebook). Airflow sirf trigger + wait karta hai.

**Manual run with options:** Trigger DAG w/ config, e.g.
`{"source": "synthetic", "ingest_date": "2027-01-01"}` (next year + dirty data + region change)
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param, task

PIPELINE_ENV = os.environ.get("PIPELINE_ENV", "local")
CLI = f"{os.environ.get('PIPELINE_BIN', 'insurance-pipeline')} --env {PIPELINE_ENV}"

default_args = {
    "owner": "data-engineering",
    # Retry: network/transient errors ke liye. KYA NAHI KARNA: 10 retries -
    # asli bug (galat data) 10 baar fail hoga aur alert der se aayega.
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="insurance_elt",
    description="Landing -> Bronze -> Silver (Spark) -> Gold (dbt)",
    doc_md=__doc__,
    # Roz 02:15 UTC. :15 - :00 par sabke jobs chalte hain, resources ki ladai.
    schedule="15 2 * * *",
    start_date=datetime(2026, 1, 1),
    # catchup=False: DAG on karte hi 2026-01-01 se aaj tak ke saare din mat chalao.
    # (Backfill chahiye to `airflow backfill create` explicitly chalao.)
    catchup=False,
    # Ek time par ek hi run - streaming checkpoints / MERGE / DuckDB file lock safe
    max_active_runs=1,
    default_args=default_args,
    params={
        "source": Param("sample", enum=["sample", "kaggle", "synthetic"],
                        description="sample=repo ki Kaggle file, kaggle=API, synthetic=next-year batch"),
        "ingest_date": Param(None, type=["null", "string"],
                             description="Business date YYYY-MM-DD (khali = run ki logical date)"),
    },
    tags=["insurance", "elt", "medallion", PIPELINE_ENV],
) as dag:

    @task
    def resolve_ingest_date(**context) -> str:
        """Business date: param > logical date > run time.
        KYUN logical date (aaj ki date nahi): rerun/backfill same din ka data same
        folder (year=/month=/day=) me likhe -> idempotent."""
        if context["params"].get("ingest_date"):
            return context["params"]["ingest_date"]
        when = context.get("logical_date") or context["dag_run"].run_after
        return when.date().isoformat()

    ingest_date = resolve_ingest_date()

    if PIPELINE_ENV == "databricks":
        from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

        # DATABRICKS MODE: data lake + poora pipeline Databricks ke andar hai
        # (inbox Volume -> ingest -> bronze -> silver -> dq -> gold dbt -> report),
        # job `insurance-medallion-pipeline` (databricks.yml). Airflow sirf trigger +
        # wait karta hai - "enterprise orchestrator" pattern (Airflow kai systems ke
        # jobs ko ek jagah se chalata hai).
        # NOTE: Databricks job ka file-arrival trigger bhi hai (PAUSED). Dono ON mat karo.
        DatabricksRunNowOperator(
            task_id="run_databricks_medallion_job",
            databricks_conn_id="databricks_default",
            job_name="insurance-medallion-pipeline",
            job_parameters={"ingest_date": "{{ ti.xcom_pull(task_ids='resolve_ingest_date') }}"},
        ).set_upstream(ingest_date)
    else:
        # LOCAL MODE: sab kuch is Docker container me (Spark local + dbt DuckDB)
        land = BashOperator(
            task_id="land_to_landing",
            doc_md="Source file -> landing/<system>/<entity>/year=/month=/day=/ + manifest (badi file auto-split)",
            bash_command=(
                f"{CLI} ingest --source {{{{ params.source }}}} "
                "--ingest-date {{ ti.xcom_pull(task_ids='resolve_ingest_date') }}"
            ),
        )
        setup = BashOperator(task_id="setup", bash_command=f"{CLI} setup")
        bronze = BashOperator(task_id="bronze", bash_command=f"{CLI} run --layer bronze")
        reconcile = BashOperator(
            task_id="reconcile_landing_vs_bronze",
            doc_md="Manifest row count == bronze rows? Mismatch = silent data loss -> FAIL",
            bash_command=f"{CLI} reconcile",
            retries=0,  # data mismatch retry se theek nahi hota
        )
        silver = BashOperator(task_id="silver", bash_command=f"{CLI} run --layer silver")
        dq_gate = BashOperator(
            task_id="dq_gate",
            doc_md="Reject % > limit -> stop (gold purane sahi data par rahe)",
            bash_command=f"{CLI} dq-gate",
            retries=0,
        )
        gold = BashOperator(
            task_id="gold_dbt_build",
            doc_md="dbt build = snapshot (SCD2) + models + tests. Koi test fail -> downstream skip.",
            bash_command=f"{CLI} run --layer gold",
        )
        docs = BashOperator(task_id="dbt_docs_generate", bash_command=f"{CLI} dbt docs generate", retries=0)
        ingest_date >> setup >> land >> bronze >> reconcile >> silver >> dq_gate >> gold >> docs
