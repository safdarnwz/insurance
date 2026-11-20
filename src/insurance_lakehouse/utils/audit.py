"""
Pipeline run audit table.

KYUN: Roz subah koi poochega "kal ka load chala? kitni rows aayi? kitni reject
hui?" - audit table ek SQL query me jawab deta hai, isi par alerting bante hain.
"""

from __future__ import annotations

import datetime as dt

from pyspark.sql import SparkSession

from ..config import PipelineConfig
from .delta_utils import write_table

AUDIT_DDL = (
    "run_id string, env string, layer string, status string, start_ts timestamp, end_ts timestamp, "
    "duration_sec long, rows_in long, rows_out long, rows_quarantined long, error_message string"
)


def write_audit(spark: SparkSession, cfg: PipelineConfig, run_id: str, layer: str, status: str,
                start: dt.datetime, end: dt.datetime, stats: dict | None, error: str | None) -> None:
    stats = stats or {}
    row = (
        run_id, cfg.env, layer, status, start, end, int((end - start).total_seconds()),
        int(stats.get("rows_in", 0)), int(stats.get("rows_out", 0)), int(stats.get("rows_quarantined", 0)),
        (error or "")[:2000],  # poora stack trace mat bharo - summary kaafi
    )
    write_table(spark.createDataFrame([row], AUDIT_DDL), cfg.table("audit", "pipeline_runs"), "append")
