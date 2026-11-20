"""
SERVING LAYER - Lakebase (Databricks managed Postgres)            [Reverse ETL]
==================================================================================

Gold Delta tables (analytics)  --synced tables-->  Lakebase Postgres (OLTP)  -->  apps / APIs

KYUN Lakebase (gold ko seedha SQL warehouse se app ko kyun nahi dete):
  * SQL warehouse = analytics engine: lakhon rows scan/aggregate ke liye best, lekin
    "ek record_id ka data do" jaisi chhoti query par bhi seconds lagte hain aur har
    query compute start karti hai.
  * Postgres = OLTP: primary key lookup milliseconds me, hazaron chhoti concurrent
    queries, standard drivers (psycopg, JDBC) - har app/backend jaanta hai.
  * Synced table: Databricks khud Delta -> Postgres copy rakhta hai (managed
    pipeline). Koi custom export script / cron nahi.

Sync mode = SNAPSHOT: dbt gold table ko har run me rebuild karta hai, isliye har
job run ke baad poora snapshot refresh (chhoti tables ke liye simple + reliable).
Badi tables par: source par Change Data Feed ON karke TRIGGERED / CONTINUOUS mode.

KYUN raw REST (`/api/2.0/postgres/...`) aur SDK ki `postgres` service nahi:
  dbt-databricks purane databricks-sdk par pinned hai; REST call kisi bhi SDK version
  par chalta hai (laptop, Docker, serverless job - sab jagah same code).

KYA NAHI KARNA:
  * App ko seedha bronze/silver ya SQL warehouse par point karna.
  * Postgres me haath se data likhna - synced table read-only copy hai; source of
    truth hamesha lakehouse (gold) hai.
  * Postgres password code me - app `generate_database_credential` se short-lived
    OAuth token leta hai (scripts/lakebase_lookup.py dekho).
"""

from __future__ import annotations

import time

from ..config import PipelineConfig
from ..utils.logger import get_logger

log = get_logger(__name__)

API = "/api/2.0/postgres"


def _client():
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()


def _get(w, path: str) -> dict | None:
    from databricks.sdk.errors import NotFound

    try:
        return w.api_client.do("GET", f"{API}/{path}")
    except NotFound:
        return None


def _scfg(cfg: PipelineConfig) -> dict:
    return cfg.raw["serving"]


def ensure_project(cfg: PipelineConfig, w=None) -> dict:
    """Lakebase project (Postgres) - na ho to banao. Idempotent."""
    w = w or _client()
    s = _scfg(cfg)
    project = _get(w, f"projects/{s['project_id']}")
    if project:
        log.info("Lakebase project exists: %s", s["project_id"])
        return project
    log.info("Lakebase project create: %s", s["project_id"])
    w.api_client.do("POST", f"{API}/projects", query={"project_id": s["project_id"]},
                    body={"spec": {"display_name": s["project_id"]}})
    for _ in range(60):  # long-running operation: ready hone tak wait
        project = _get(w, f"projects/{s['project_id']}")
        if project and project.get("status", {}).get("default_branch"):
            return project
        time.sleep(5)
    raise TimeoutError("Lakebase project ready nahi hua")


def _synced_name(cfg: PipelineConfig, source: str) -> str:
    return f"{cfg.catalog}.{_scfg(cfg)['uc_schema']}.{source}"


def ensure_synced_tables(cfg: PipelineConfig, w=None) -> list[str]:
    """Har gold table ke liye synced table (Delta -> Postgres). Na ho to banao."""
    w = w or _client()
    s = _scfg(cfg)
    created = []
    for t in s["synced_tables"]:
        name = _synced_name(cfg, t["source"])
        if _get(w, f"synced_tables/{name}"):
            continue
        log.info("Synced table create: %s.%s -> Postgres", cfg.schemas["gold"], t["source"])
        w.api_client.do("POST", f"{API}/synced_tables", query={"synced_table_id": name}, body={"spec": {
            "source_table_full_name": f"{cfg.catalog}.{cfg.schemas['gold']}.{t['source']}",
            "branch": f"projects/{s['project_id']}/branches/{s['branch']}",
            "postgres_database": s["postgres_database"],
            "primary_key_columns": t["primary_key"],
            "scheduling_policy": "SNAPSHOT",
            "create_database_objects_if_missing": True,
            "new_pipeline_spec": {"storage_catalog": cfg.catalog, "storage_schema": s["uc_schema"]},
        }})
        created.append(name)
    return created


def _wait_online(w, name: str, timeout_s: int = 1200) -> dict:
    t0 = time.time()
    while time.time() - t0 < timeout_s:
        status = (_get(w, f"synced_tables/{name}") or {}).get("status", {})
        state = status.get("detailed_state", "")
        if "FAILED" in state:
            raise RuntimeError(f"{name}: {state} {status.get('message', '')[:300]}")
        if "ONLINE" in state and "PROVISIONING" not in state:
            return status
        time.sleep(15)
    raise TimeoutError(f"{name} ONLINE nahi hua")


def sync(cfg: PipelineConfig) -> dict:
    """Job ka serving step: project + synced tables ensure, phir har table ka snapshot refresh.
    Gold (dbt) ke BAAD chalta hai, taaki Postgres me hamesha latest gold ho."""
    w = _client()
    ensure_project(cfg, w)
    new = set(ensure_synced_tables(cfg, w))
    result = {}
    for t in _scfg(cfg)["synced_tables"]:
        name = _synced_name(cfg, t["source"])
        status = _wait_online(w, name)
        if name not in new:
            # SNAPSHOT refresh = synced table ki managed pipeline ka naya update
            pipeline_id = status["pipeline_id"]
            update = w.pipelines.start_update(pipeline_id)
            log.info("Snapshot refresh started: %s (update %s)", name, update.update_id)
            for _ in range(80):
                st = w.pipelines.get_update(pipeline_id, update.update_id).update.state.value
                if st in ("COMPLETED", "FAILED", "CANCELED"):
                    break
                time.sleep(15)
            if st != "COMPLETED":
                raise RuntimeError(f"{name} refresh {st}")
            status = _wait_online(w, name)
        last = (status.get("last_sync") or {}).get("delta_table_sync_info", {})
        result[t["source"]] = {"state": status.get("detailed_state"), "delta_version": last.get("delta_commit_version")}
        log.info("SERVING %s -> Postgres: %s (delta version %s synced)", t["source"],
                 status.get("detailed_state"), last.get("delta_commit_version"))
    return result
