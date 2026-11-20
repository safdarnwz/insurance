"""
GOLD LAYER = dbt                                              [ELT ka "T" - part 2]

Python se dbt CLI chalata hai, sahi environment variables ke saath:
  local      -> dbt-duckdb  : silver Delta tables padh ke DuckDB warehouse me gold
  databricks -> dbt-databricks : Databricks SQL warehouse par UC tables me gold

KYUN subprocess (dbt ka Python API nahi): `dbt` CLI hi stable public interface hai;
Airflow, CI, VS Code terminal - sab jagah same command chalti hai.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

from ..config import PROJECT_ROOT, PipelineConfig
from ..utils.logger import get_logger

log = get_logger(__name__)

DBT_DIR = PROJECT_ROOT / "dbt"
# Wheel ke andar packaged copy (databricks.yml ka artifact build ise copy karta hai)
PACKAGED_DBT_DIR = Path(__file__).resolve().parents[1] / "_dbt_project"


def resolve_dbt_dir() -> Path:
    """Repo checkout (laptop/Docker) -> ./dbt.  Databricks job (wheel) -> packaged copy
    ko temp folder me copy (site-packages read-only ho sakta hai, dbt target/ likhta hai)."""
    if (DBT_DIR / "dbt_project.yml").exists():
        return DBT_DIR
    if (PACKAGED_DBT_DIR / "dbt_project.yml").exists():
        import tempfile

        tmp = Path(tempfile.mkdtemp(prefix="dbt_")) / "dbt"
        shutil.copytree(PACKAGED_DBT_DIR, tmp)
        return tmp
    raise RuntimeError("dbt project nahi mila (./dbt ya packaged _dbt_project)")


def _databricks_runtime_auth(env: dict) -> None:
    """Databricks job ke andar token .env me nahi hota. Databricks SDK runtime ki
    apni identity (job run-as user) se short-lived auth header deta hai -> dbt use karta hai.
    KYA NAHI KARNA: job me PAT hardcode / job parameter me token bhejna."""
    if env.get("DATABRICKS_TOKEN") or "DATABRICKS_RUNTIME_VERSION" not in env:
        return
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    env["DATABRICKS_HOST"] = w.config.host
    env["DATABRICKS_TOKEN"] = w.config.authenticate()["Authorization"].split(" ", 1)[1]


def dbt_env(cfg: PipelineConfig, dbt_dir: Path = DBT_DIR) -> dict:
    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = str(dbt_dir)  # profiles.yml repo me (secrets env vars se aate hain)
    env["DBT_SILVER_SCHEMA"] = cfg.schemas["silver"]
    env["DBT_GOLD_SCHEMA"] = cfg.schemas["gold"]
    if cfg.is_local:
        env["DUCKDB_PATH"] = cfg.warehouse_path
        env["SILVER_PATH"] = cfg.storage["silver"]
    else:
        env["UC_CATALOG"] = cfg.catalog
        _databricks_runtime_auth(env)
    return env


def _dbt_executable() -> str:
    exe = os.environ.get("DBT_EXECUTABLE") or shutil.which("dbt")
    if not exe:
        candidate = Path(sys.executable).parent / "dbt"
        exe = str(candidate) if candidate.exists() else None
    if not exe:
        raise RuntimeError("dbt nahi mila. `pip install -r requirements.txt` karo.")
    return exe


def run_dbt(cfg: PipelineConfig, args: list[str]) -> dict:
    if cfg.is_local and cfg.warehouse_path:
        Path(cfg.warehouse_path).parent.mkdir(parents=True, exist_ok=True)
    dbt_dir = resolve_dbt_dir()
    cmd = [_dbt_executable(), *args, "--project-dir", str(dbt_dir), "--target", cfg.dbt_target]
    log.info("dbt: %s", " ".join(cmd[1:]))
    proc = subprocess.run(cmd, env=dbt_env(cfg, dbt_dir), cwd=str(dbt_dir))
    results = {}
    rr = dbt_dir / "target" / "run_results.json"
    if rr.exists():
        for r in json.loads(rr.read_text())["results"]:
            results[r["status"]] = results.get(r["status"], 0) + 1
    if proc.returncode != 0:
        raise RuntimeError(f"dbt failed (exit {proc.returncode}): {results}")
    return results


def run_gold(cfg: PipelineConfig) -> dict:
    """dbt build = models + snapshots + tests, dependency order me (ek test fail -> downstream skip)."""
    results = run_dbt(cfg, ["build"])
    log.info("GOLD (dbt build) done: %s", results)
    return {"layer": "gold", "rows_in": 0, "rows_out": sum(results.values()), "rows_quarantined": 0}
