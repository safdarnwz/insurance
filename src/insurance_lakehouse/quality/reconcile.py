"""
Reconciliation: Landing manifest ki row count == Bronze me load hui rows?

KYUN: Pipeline "SUCCESS" ho gayi par 1 TB me se 2 files padhi hi nahi gayi (permission,
glob galat, file upload adhoora) - ye silent data loss sabse khatarnak bug hai.
Manifest (landing.py likhta hai) source ka "promise" hai; bronze actual hai.
Mismatch = pipeline FAIL, gold tak galat data nahi jaata.

Industry naam: "control totals" / "record count reconciliation" (banking/insurance
audit me mandatory hota hai).
"""

from __future__ import annotations

import glob
import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ..config import PipelineConfig
from ..utils.delta_utils import read_table, table_exists
from ..utils.logger import get_logger

log = get_logger(__name__)


def expected_from_manifests(cfg: PipelineConfig, source: str) -> dict[str, int]:
    """{part_file_name: rows} saare manifests se. /Volumes bhi local file jaisa padhta hai (Databricks)."""
    expected: dict[str, int] = {}
    for mf in glob.glob(f"{cfg.landing_dir(source)}/year=*/month=*/day=*/_manifest_*.json"):
        with open(mf, encoding="utf-8") as fh:
            for part in json.load(fh)["parts"]:
                expected[part["file"]] = int(part["rows"])
    return expected


def reconcile(spark: SparkSession, cfg: PipelineConfig) -> dict:
    problems: list[str] = []
    summary = {}
    for source, scfg in cfg.sources.items():
        expected = expected_from_manifests(cfg, source)
        ref = cfg.table("bronze", scfg["bronze_table"])
        actual: dict[str, int] = {}
        if table_exists(spark, ref):
            actual = {
                r["_source_file_name"]: r["n"]
                for r in read_table(spark, ref).groupBy("_source_file_name").agg(F.count("*").alias("n")).collect()
            }
        for f, n in expected.items():
            got = actual.get(f)
            if got is None:
                problems.append(f"{source}: {f} manifest me hai par bronze me NAHI (expected {n})")
            elif got != n:
                problems.append(f"{source}: {f} expected {n} rows, bronze {got}")
        extra = set(actual) - set(expected)
        if extra:
            # Bina manifest ki file - warn (kisi ne haath se file daali?)
            log.warning("%s: %d bronze file(s) ka manifest nahi: %s", source, len(extra), sorted(extra)[:3])
        summary[source] = {"files": len(expected), "rows": sum(expected.values())}

    if problems:
        for p in problems:
            log.error("RECONCILE: %s", p)
        raise RuntimeError(f"Reconciliation FAILED: {len(problems)} mismatch(es)")
    log.info("RECONCILE OK: %s", summary)
    return summary
