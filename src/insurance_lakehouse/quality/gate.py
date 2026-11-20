"""
DQ gate ("circuit breaker"): silver ke baad, gold (dbt) se pehle.

Agar last silver run me reject % threshold (config: quality_gate.max_reject_pct)
se zyada hai to pipeline FAIL -> dbt nahi chalega -> dashboards purane (sahi) data
par rahenge, aur Airflow alert bhejega.

KYUN: Source ne aadhi kharab file bheji (column shift, encoding issue) to 40% rows
quarantine me jaayengi aur baaki 60% se "charges 40% gir gaye" jaisi galat report
banegi. Galat number dikhane se behtar hai number update hi na ho.
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ..config import PipelineConfig
from ..utils.delta_utils import read_table, table_exists
from ..utils.logger import get_logger

log = get_logger(__name__)


def dq_gate(spark: SparkSession, cfg: PipelineConfig, max_reject_pct: float | None = None) -> float:
    limit = max_reject_pct if max_reject_pct is not None else float(cfg.quality_gate["max_reject_pct"])
    ref = cfg.table("audit", "pipeline_runs")
    if not table_exists(spark, ref):
        raise RuntimeError("audit.pipeline_runs nahi mili - pehle silver chalao")
    last = (
        read_table(spark, ref).filter((F.col("layer") == "silver") & (F.col("status") == "SUCCESS"))
        .orderBy(F.col("start_ts").desc()).first()
    )
    if last is None or not last.rows_in:
        log.info("DQ GATE: last silver run me naya data nahi - pass")
        return 0.0
    pct = 100.0 * last.rows_quarantined / last.rows_in
    log.info("DQ GATE: rejected %d / %d = %.2f%% (limit %.2f%%)", last.rows_quarantined, last.rows_in, pct, limit)
    if pct > limit:
        raise RuntimeError(f"DQ GATE FAILED: {pct:.2f}% rows rejected (> {limit}%). Quarantine table dekho.")
    return pct
