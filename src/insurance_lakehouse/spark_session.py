"""
SparkSession factory - do jagah chalna hai:

1. Databricks (Free Edition serverless job) -> Spark pehle se hai, bas getOrCreate().
2. Local (laptop / Docker) -> PySpark + delta-spark pip package.

KYA NAHI KARNA: Databricks par apna SparkSession `.master("local")` ke saath
banane ki koshish mat karo. Serverless par zyada tar Spark configs set hi nahi
hote (CONFIG_NOT_AVAILABLE error) - isliye `_safe_set` use kiya hai.
"""

from __future__ import annotations

import glob
import os

from pyspark.sql import SparkSession

from .config import PipelineConfig
from .utils.logger import get_logger

log = get_logger(__name__)


def is_databricks_runtime() -> bool:
    # Databricks cluster / serverless par ye env variable hamesha set hota hai
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def _safe_set(spark: SparkSession, key: str, value: str) -> None:
    try:
        spark.conf.set(key, value)
    except Exception as exc:  # serverless: sirf kuch confs allowed hain
        log.warning("Spark conf %s set nahi hua (serverless me allowed nahi?): %s", key, str(exc)[:120])


def get_spark(cfg: PipelineConfig, app_name: str = "insurance-lakehouse") -> SparkSession:
    if is_databricks_runtime():
        log.info("Databricks runtime - existing SparkSession use kar rahe hain")
        spark = SparkSession.builder.getOrCreate()
    else:
        spark = _build_local_spark(cfg, app_name)

    # UTC: KYUN - server alag timezone me ho to timestamps shift ho jaate hain.
    _safe_set(spark, "spark.sql.session.timeZone", "UTC")
    if cfg.shuffle_partitions != "auto":
        _safe_set(spark, "spark.sql.shuffle.partitions", cfg.shuffle_partitions)
    return spark


def _delta_jars_bundled() -> bool:
    """Docker image me delta jars pehle se pyspark/jars me copy hote hain
    (runtime par internet se download nahi - company standard: reproducible build)."""
    import pyspark

    return bool(glob.glob(os.path.join(os.path.dirname(pyspark.__file__), "jars", "delta-spark_*.jar")))


def _build_local_spark(cfg: PipelineConfig, app_name: str) -> SparkSession:
    log.info("Local SparkSession (delta-spark) bana rahe hain")
    lf = cfg.large_files
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        # Ye 2 configs Delta Lake ko Spark SQL me plug karte hain (MERGE, OPTIMIZE,
        # time travel). Databricks par ye already enabled hote hain.
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Laptop par 200 shuffle partitions (default) chhote data ke liye zyada -> tiny files
        .config("spark.sql.shuffle.partitions", cfg.shuffle_partitions)
        # Ek input partition (task) max kitna data padhe: 1 TB / 128 MB = ~8192 tasks
        .config("spark.sql.files.maxPartitionBytes", str(lf["max_partition_bytes"]))
        # Delta optimized writes: write se pehle data ko kam, bade files me jodta hai
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.ui.showConsoleProgress", "false")
        # Local Spark ka metastore/warehouse project ke data/ folder me (repo gandi na ho)
        .config("spark.sql.warehouse.dir", os.path.join(os.path.dirname(cfg.storage["bronze"]), "_spark_warehouse"))
    )
    if _delta_jars_bundled():
        spark = builder.getOrCreate()
    else:
        from delta import configure_spark_with_delta_pip

        # Pehli baar Maven se delta jars download karta hai (~5 MB, internet chahiye)
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
