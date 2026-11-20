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


def dbt_env(cfg: PipelineConfig) -> dict:
    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = str(DBT_DIR)  # profiles.yml repo me (secrets env vars se aate hain)
    env["DBT_SILVER_SCHEMA"] = cfg.schemas["silver"]
    env["DBT_GOLD_SCHEMA"] = cfg.schemas["gold"]
    if cfg.is_local:
        env["DUCKDB_PATH"] = cfg.warehouse_path
        env["SILVER_PATH"] = cfg.storage["silver"]
    else:
        env["UC_CATALOG"] = cfg.catalog
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
    cmd = [_dbt_executable(), *args, "--project-dir", str(DBT_DIR), "--target", cfg.dbt_target]
    log.info("dbt: %s", " ".join(cmd[1:]))
    proc = subprocess.run(cmd, env=dbt_env(cfg), cwd=str(DBT_DIR))
    results = {}
    rr = DBT_DIR / "target" / "run_results.json"
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
