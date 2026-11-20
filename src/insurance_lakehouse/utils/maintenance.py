"""
Setup (schemas/volumes) + Delta maintenance (OPTIMIZE / Z-ORDER / VACUUM).

OPTIMIZE KYUN: streaming / MERGE se bahut chhoti files banti hain ("small file
problem") -> har query hazaron files kholti hai. OPTIMIZE unhe bade files me jodta hai.
Z-ORDER: related values ek saath -> filter queries kam files padhti hain.
VACUUM: purani unreferenced files delete -> storage cost kam.

KYA NAHI KARNA:
  * VACUUM RETAIN 0 HOURS - time travel khatam, chal rahi queries fail. Default 7 din.
  * Har pipeline run ke baad OPTIMIZE - alag weekly job/DAG rakho.
Databricks par Predictive Optimization ye sab khud bhi kar deta hai.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession

from ..config import PipelineConfig
from .delta_utils import delta_table, table_exists
from .logger import get_logger

log = get_logger(__name__)

ZORDER = {
    ("silver", "member_charges"): ["region", "policy_year"],
}


def setup(spark: SparkSession | None, cfg: PipelineConfig) -> None:
    """One-time / idempotent setup. Databricks: schemas + volume. Local: folders."""
    if cfg.table_mode == "catalog":
        cat = cfg.catalog
        for layer in ("landing", "bronze", "silver", "gold", "audit"):
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cat}.{cfg.schemas[layer]}")
        # Volume = UC governed folder: landing files + streaming checkpoints
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {cat}.{cfg.schemas['landing']}.files")
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {cat}.{cfg.schemas['landing']}.checkpoints")
        log.info("Setup done: catalog %s, schemas + volumes ready", cat)
    else:
        for key in ("landing", "bronze", "silver", "audit", "checkpoints"):
            Path(cfg.storage[key]).mkdir(parents=True, exist_ok=True)
        if cfg.warehouse_path:
            Path(cfg.warehouse_path).parent.mkdir(parents=True, exist_ok=True)
        log.info("Setup done: local folders ready")


def run_maintenance(spark: SparkSession, cfg: PipelineConfig, retain_hours: int = 168) -> None:
    for layer, tables in cfg.tables.items():
        for key in tables:
            ref = cfg.table(layer, key)
            if not table_exists(spark, ref):
                continue
            dt = delta_table(spark, ref)
            zcols = ZORDER.get((layer, key))
            if zcols:
                dt.optimize().executeZOrderBy(*zcols)
            else:
                dt.optimize().executeCompaction()
            dt.vacuum(retain_hours)
            log.info("OPTIMIZE%s + VACUUM -> %s", f" ZORDER BY {zcols}" if zcols else "", ref)
