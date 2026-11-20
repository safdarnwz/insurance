"""
Pipeline orchestrator: bronze -> silver -> gold(dbt), har step audit ke saath.

Airflow (airflow/dags/) har layer ko alag task ki tarah chalata hai - KYUN: ek
layer fail ho to sirf wahi retry ho, aur har step ka alag log/duration dikhe.
Ye function local/CLI se "sab ek saath" chalane ke liye hai.
"""

from __future__ import annotations

import datetime as dt
import uuid

from pyspark.sql import SparkSession

from .config import PipelineConfig
from .utils.logger import get_logger

log = get_logger(__name__)


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)


def run_layer(spark: SparkSession | None, cfg: PipelineConfig, layer: str) -> dict:
    if layer == "bronze":
        from .bronze.ingest_bronze import run_bronze

        return run_bronze(spark, cfg)
    if layer == "silver":
        from .silver.transform_silver import run_silver

        return run_silver(spark, cfg)
    if layer == "gold":
        from .gold.dbt_runner import run_gold

        return run_gold(cfg)
    raise ValueError(layer)


def run_layers(spark: SparkSession | None, cfg: PipelineConfig, layers: list[str],
               run_id: str | None = None) -> list[dict]:
    from .utils.audit import write_audit

    run_id = run_id or uuid.uuid4().hex[:12]
    results = []
    for layer in layers:
        start = _now()
        try:
            stats = run_layer(spark, cfg, layer)
            if spark is not None:
                write_audit(spark, cfg, run_id, layer, "SUCCESS", start, _now(), stats, None)
            results.append(stats)
        except Exception as exc:
            if spark is not None:
                write_audit(spark, cfg, run_id, layer, "FAILED", start, _now(), None, repr(exc))
            log.error("Layer %s FAILED: %s", layer, exc)
            # KYA NAHI KARNA: exception "swallow" karke job green mat dikhao.
            # Airflow ko failure pata chalna chahiye -> retry + alert + downstream skip.
            raise
    return results
