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


def setup_statements(cfg: PipelineConfig) -> list[str]:
    cat = cfg.catalog
    stmts = [f"CREATE SCHEMA IF NOT EXISTS {cat}.{cfg.schemas[layer]}"
             for layer in ("landing", "bronze", "silver", "gold", "audit", "serving")]
    # Volume = UC governed folder: landing files + streaming checkpoints
    stmts += [f"CREATE VOLUME IF NOT EXISTS {cat}.{cfg.schemas['landing']}.inbox",
              f"CREATE VOLUME IF NOT EXISTS {cat}.{cfg.schemas['landing']}.files",
              # Code artifacts (wheel) - bundle deploy yahan upload karta hai
              f"CREATE VOLUME IF NOT EXISTS {cat}.{cfg.schemas['landing']}.artifacts",
              f"CREATE VOLUME IF NOT EXISTS {cat}.{cfg.schemas['landing']}.checkpoints"]
    return stmts


SCHEMA_DOCS = {
    "landing": "Volumes: inbox (source drop zone), files (standard-named landing + manifests), "
               "checkpoints, artifacts (code wheels)",
    "bronze": "Raw data as received (all STRING), append-only, Auto Loader",
    "silver": "Cleaned, typed, validated (DQ), deduplicated. Quarantine table for rejects",
    "gold": "Business star schema + KPIs built by dbt. Use this for BI / dashboards",
    "audit": "Pipeline run audit + batch statistics",
    "serving": "Lakebase synced tables: gold copies in Postgres for app/API lookups (read-only)",
}


def governance_statements(cfg: PipelineConfig) -> list[str]:
    """Unity Catalog governance: schema COMMENTS + TAGS -> Catalog Explorer me
    searchable/discoverable (data catalog standard). Health data = sensitive tag."""
    cat = cfg.catalog
    out = []
    for layer, doc in SCHEMA_DOCS.items():
        fq = f"{cat}.{cfg.schemas[layer]}"
        out.append(f"COMMENT ON SCHEMA {fq} IS '{doc}'")
        tags = f"'domain' = 'insurance', 'layer' = '{layer}'"
        if layer in ("bronze", "silver", "gold"):
            tags += ", 'data_classification' = 'sensitive_health'"
        out.append(f"ALTER SCHEMA {fq} SET TAGS ({tags})")
    return out


def _run_on_sql_warehouse(statements: list[str]) -> None:
    """Laptop/Airflow se (bina Spark) Databricks SQL warehouse par DDL.
    KYUN: landing upload se PEHLE Volume chahiye, aur Spark job baad me chalta hai."""
    import os

    from databricks.sdk import WorkspaceClient

    warehouse_id = os.environ.get("DATABRICKS_HTTP_PATH", "").rstrip("/").split("/")[-1]
    if not warehouse_id or "<" in warehouse_id:
        raise RuntimeError("DATABRICKS_HTTP_PATH .env me set karo (SQL warehouse ka HTTP path)")
    w = WorkspaceClient()
    for q in statements:
        r = w.statement_execution.execute_statement(statement=q, warehouse_id=warehouse_id, wait_timeout="50s")
        if r.status.state.value != "SUCCEEDED":
            raise RuntimeError(f"{q} -> {r.status.state.value}: {r.status.error}")


def setup(spark: SparkSession | None, cfg: PipelineConfig) -> None:
    """One-time / idempotent setup. Databricks: schemas + volumes. Local: folders."""
    if cfg.table_mode == "catalog":
        stmts = setup_statements(cfg)
        if spark is not None:      # Databricks job ke andar
            for q in stmts:
                spark.sql(q)
        else:                      # laptop / Airflow container se
            _run_on_sql_warehouse(stmts)
        # Governance best-effort: tags kisi workspace me allowed na hon to pipeline na ruke
        for q in governance_statements(cfg):
            try:
                spark.sql(q) if spark is not None else _run_on_sql_warehouse([q])
            except Exception as exc:
                log.warning("Governance statement skip: %s (%s)", q[:60], str(exc)[:100])
        log.info("Setup done: catalog %s, schemas + volumes ready", cfg.catalog)
    else:
        for key in ("inbox", "landing", "bronze", "silver", "audit", "checkpoints"):
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
