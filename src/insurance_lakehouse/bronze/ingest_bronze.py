"""
BRONZE LAYER  (Landing CSV files  ->  bronze.<source>_raw Delta)      [ELT ka "L"]
==================================================================================

Bronze ka rule: "Raw, append-only, jaisa aaya waisa".
  * Koi business logic / filter / dedupe nahi.
  * Sirf technical metadata: kis file se, kab, kis business date ka.
  * Sab columns STRING (schemas.py me reason).

INCREMENTAL (sirf nayi files) - Structured Streaming + checkpoint + availableNow:
  - Checkpoint yaad rakhta hai kaunsi files process ho chuki hain.
  - availableNow = jitna naya data hai process karo aur ruk jao (batch jaisa,
    par exactly-once guarantee). Databricks serverless par yahi recommended trigger hai.
  - Databricks par Auto Loader (cloudFiles): lakhon files me bhi fast listing,
    schema evolution (_rescued_data).

1 TB DATA (docs/LARGE_FILES.md):
  - Landing par file 128 MB parts me split hoti hai -> ~8000 files.
  - maxFilesPerTrigger=100 -> ek micro-batch me 100 files (~12.8 GB). Har batch
    commit hota hai: beech me fail hua to sirf wahi batch dobara, poora TB nahi.
  - Spark har file ko maxPartitionBytes (128 MB) ke tasks me padhta hai -> parallel.
  - Partition by _ingest_date: replay/debug "us din ka data" se hota hai.

KYA NAHI KARNA:
  * `spark.read.csv(landing).write.mode("overwrite")` - har run me poora history
    dobara padhna + overwrite = slow, mehenga, purana data gayab.
  * Checkpoint delete karke "fix" - saari files dobara aayengi -> duplicates.
  * gzip ki 1 TB file - gzip SPLITTABLE nahi hai, poori file EK task padhega.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..config import PipelineConfig
from ..schemas import CORRUPT_RECORD_COL, raw_schema
from ..utils.delta_utils import start_stream
from ..utils.logger import get_logger

log = get_logger(__name__)


def _read_landing_stream(spark: SparkSession, cfg: PipelineConfig, source: str) -> DataFrame:
    lf = cfg.large_files
    opts = {
        "header": "true",
        # PERMISSIVE: kharab line par job fail nahi, wo _corrupt_record me jaayegi.
        # (FAILFAST ek line ki wajah se poora load rokta hai; DROPMALFORMED
        #  chupchaap data gayab karta hai - dono avoid karo.)
        "mode": "PERMISSIVE",
        "columnNameOfCorruptRecord": CORRUPT_RECORD_COL,
        "escape": '"',
        # Sirf data files - _manifest_*.json waghera ignore
        "pathGlobFilter": "*.csv",
    }
    if cfg.use_autoloader:
        reader = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation", cfg.checkpoint(f"bronze_{source}_schema"))
            # Hum schema dete hain (contract); naye unexpected columns _rescued_data me
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            # year=/month=/day= folders ko columns banao
            .option("cloudFiles.partitionColumns", "year,month,day")
            .option("cloudFiles.maxFilesPerTrigger", str(lf["max_files_per_trigger"]))
        )
    else:
        reader = spark.readStream.format("csv").option("maxFilesPerTrigger", lf["max_files_per_trigger"])

    for k, v in opts.items():
        reader = reader.option(k, v)
    return reader.schema(raw_schema(source)).load(cfg.landing_dir(source))


def add_bronze_metadata(df: DataFrame) -> DataFrame:
    """Lineage columns. Underscore prefix = technical column (business column se alag)."""
    for c in ("year", "month", "day"):
        if c in df.columns:
            df = df.withColumn(f"_landing_{c}", F.col(c).cast("int")).drop(c)
    return (
        df.withColumn(
            "_landing_date",  # business date (folder se)
            F.make_date(F.col("_landing_year"), F.col("_landing_month"), F.col("_landing_day")),
        )
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_source_file_name", F.col("_metadata.file_name"))
        # File modification time - silver me "latest record wins" ke liye
        .withColumn("_source_file_ts", F.col("_metadata.file_modification_time"))
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_ingest_date", F.to_date(F.col("_ingest_ts")))
    )


def _num_rows(progress) -> int:
    # Local Spark: dict; Spark Connect (serverless): object - dono handle
    total = 0
    for p in progress or []:
        total += p.get("numInputRows", 0) if isinstance(p, dict) else getattr(p, "numInputRows", 0)
    return int(total)


def ingest_source(spark: SparkSession, cfg: PipelineConfig, source: str) -> int:
    ref = cfg.table("bronze", cfg.sources[source]["bronze_table"])
    log.info("BRONZE %s: %s -> %s", source, cfg.landing_dir(source), ref)
    writer = (
        add_bronze_metadata(_read_landing_stream(spark, cfg, source))
        .writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", cfg.checkpoint(f"bronze_{source}"))
        # KYA NAHI KARNA: high-cardinality column (member id) par partition -
        # lakhon chhote folders/files ban jaate hain. Date jaisa low-cardinality column lo.
        .partitionBy("_ingest_date")
        .trigger(availableNow=True)
        .queryName(f"bronze_{source}")
    )
    query = start_stream(writer, ref)
    query.awaitTermination()
    rows = _num_rows(query.recentProgress)
    log.info("BRONZE %s: %d new rows appended", source, rows)
    return rows


def run_bronze(spark: SparkSession, cfg: PipelineConfig) -> dict:
    rows = sum(ingest_source(spark, cfg, s) for s in cfg.sources)
    return {"layer": "bronze", "rows_in": rows, "rows_out": rows, "rows_quarantined": 0}
