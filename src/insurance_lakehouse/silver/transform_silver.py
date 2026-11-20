"""
SILVER LAYER  (Spark)                                        [ELT ka "T" - part 1]
=================================================================================
  bronze.region_master_raw   ->  silver.region_master             (latest per region)
  bronze.member_charges_raw  ->  silver.member_charges            (clean, validated, deduped)
                             ->  silver.member_charges_quarantine (DQ fail rows + reason)

Silver ka rule: "Clean, validated, deduplicated - ek trusted version".
  1. Standardize : trim, lower-case, '' / 'NA' -> NULL
  2. Type cast   : string -> int/decimal/boolean (try_cast -> fail nahi, NULL)
  3. Derive      : record_id, policy_year, landing_date, age_band, bmi_category
  4. DQ rules    : fail -> quarantine, pass -> silver
  5. Dedupe      : exact duplicates ek baar
  6. MERGE       : record_id par (incremental, idempotent)

KYUN silver Spark me aur gold dbt me?
  Silver = heavy row-level kaam (TB scale parsing, casting, dedupe, DQ) -> Spark
  distributed engine best. Gold = business modelling (joins, SCD2, KPIs) -> SQL
  me likhna/test/document karna aasaan -> dbt. Ye company me common split hai.

SERVERLESS NOTE (Databricks Free Edition): foreachBatch function alag process me
chalta hai. Isliye: (a) outer `spark` ki jagah `batch_df.sparkSession` use karo,
(b) Python list me stats jama nahi ho sakte -> stats ek Delta table me likhte hain.

KYA NAHI KARNA:
  * Silver me KPI/aggregation - wo gold (dbt) ka kaam hai.
  * Paise ko DOUBLE me - 0.1 + 0.2 = 0.30000000000000004. DECIMAL use karo.
  * dropDuplicates() bina soche - pehle decide karo "duplicate" ka matlab kya hai.
"""

from __future__ import annotations

import datetime as dt

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from ..config import PipelineConfig
from ..quality.dq_checks import apply_dq, member_rules
from ..schemas import CORRUPT_RECORD_COL, MEMBER_COLUMNS, REGION_COLUMNS
from ..utils.delta_utils import merge_upsert, read_stream, read_table, table_exists, write_table
from ..utils.logger import get_logger

log = get_logger(__name__)


def _clean_str(col: str) -> F.Column:
    """trim + blank/'NA'/'NULL'/'?' -> NULL."""
    c = F.trim(F.col(col))
    return F.when(F.upper(c).isin("", "?", "NA", "N/A", "NULL"), None).otherwise(c)


def _hash(cols: list[str]) -> F.Column:
    return F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("~")) for c in cols]), 256)


# =============================================================== REGION MASTER
REGION_ATTRS = ["region_name", "country", "regional_office", "risk_zone"]


def clean_region_master(df: DataFrame) -> DataFrame:
    df = df.filter(F.col(CORRUPT_RECORD_COL).isNull())
    for c in REGION_COLUMNS:
        df = df.withColumn(c, _clean_str(c))
    df = df.withColumn("region", F.lower("region")).filter(F.col("region").isNotNull())
    # Latest file wins (reference data me har nayi file = poori latest list)
    w = Window.partitionBy("region").orderBy(F.col("_source_file_ts").desc(), F.col("_source_file").desc())
    return (
        df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")
        .select(
            "region", *REGION_ATTRS,
            # effective_date = jis business date ki file me ye value aayi.
            # MERGE sirf CHANGE par update karta hai -> unchanged regions ki date
            # purani hi rehti hai. dbt snapshot isi se SCD2 versions banata hai.
            F.col("_landing_date").alias("effective_date"),
            "_source_file",
        )
        .withColumn("_row_hash", _hash(REGION_ATTRS))
        .withColumn("_silver_updated_ts", F.current_timestamp())
    )


def run_region_master(spark: SparkSession, cfg: PipelineConfig) -> list[str]:
    """Reference data chhota hai (4 rows) -> streaming nahi, har run full read + MERGE."""
    src = cfg.table("bronze", "region_master_raw")
    if not table_exists(spark, src):
        raise RuntimeError("bronze.region_master_raw nahi mili - pehle bronze chalao")
    ref = cfg.table("silver", "region_master")
    merge_upsert(spark, clean_region_master(read_table(spark, src)), ref, ["region"],
                 change_condition="s._row_hash <> t._row_hash")
    regions = [r.region for r in read_table(spark, ref).select("region").collect()]
    log.info("SILVER region_master: %s", regions)
    return regions


# ============================================================= MEMBER CHARGES
def clean_members(df: DataFrame) -> DataFrame:
    """Pure transformation (no I/O) - isliye unit test ho sakta hai (tests/ dekho)."""
    # Original values JSON me - quarantine me dikhe ki source ne kya bheja tha
    df = df.withColumn("_raw_record", F.to_json(F.struct(*MEMBER_COLUMNS)))

    for c in MEMBER_COLUMNS:
        df = df.withColumn(c, _clean_str(c))
    df = df.withColumn("sex", F.lower("sex")).withColumn("region", F.lower("region")).withColumn(
        "smoker", F.lower("smoker")
    )

    # try_cast: "abc" -> NULL (exception nahi; Databricks par ANSI mode ON hota hai,
    # wahan plain cast job fail kar deta). Fir DQ rule pakdega.
    df = (
        df.withColumn("age", F.expr("try_cast(age AS INT)"))
        .withColumn("children", F.expr("try_cast(children AS INT)"))
        .withColumn("bmi", F.expr("try_cast(bmi AS DECIMAL(5,2))"))
        .withColumn("charges", F.expr("try_cast(charges AS DECIMAL(12,2))"))
        .withColumn(
            "is_smoker",
            F.when(F.col("smoker").isin("yes", "y", "true"), True)
            .when(F.col("smoker").isin("no", "n", "false"), False),
        )
    )

    landing_date = F.col("_landing_date") if "_landing_date" in df.columns else F.to_date("_ingest_ts")
    df = (
        df.withColumn("landing_date", landing_date)
        .withColumn("policy_year", F.year("landing_date"))
        .withColumn(
            "age_band",
            F.when(F.col("age") < 25, "18-24").when(F.col("age") < 35, "25-34")
            .when(F.col("age") < 45, "35-44").when(F.col("age") < 55, "45-54")
            .when(F.col("age") < 65, "55-64").when(F.col("age").isNotNull(), "65+"),
        )
        # WHO BMI categories - underwriting standard
        .withColumn(
            "bmi_category",
            F.when(F.col("bmi") < 18.5, "Underweight").when(F.col("bmi") < 25, "Normal")
            .when(F.col("bmi") < 30, "Overweight").when(F.col("bmi").isNotNull(), "Obese"),
        )
        # record_id: dataset me member_id / policy_id NAHI hai -> business columns
        # + policy_year ka hash = deterministic key. Same file dobara aaye to MERGE
        # duplicate nahi banata. REAL PROJECT: source se proper ID maango.
        .withColumn(
            "record_id",
            _hash(["policy_year", "age", "sex", "bmi", "children", "is_smoker", "region", "charges"]),
        )
    )
    return df


SILVER_COLUMNS = [
    "record_id", "policy_year", "landing_date", "age", "age_band", "sex", "bmi", "bmi_category",
    "children", "smoker", "is_smoker", "region", "charges", "dq_warning_count", "_dq_warnings",
    "_source_file", "_source_file_ts", "_ingest_ts",
]
QUARANTINE_COLUMNS = ["record_id", "_dq_errors", "_raw_record", "_source_file", "_ingest_ts"]


def dedupe(df: DataFrame) -> DataFrame:
    """Same record_id ke multiple rows -> ek (latest file wala)."""
    w = Window.partitionBy("record_id").orderBy(F.col("_source_file_ts").desc(), F.col("_source_file").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def _process_batch(cfg: PipelineConfig, regions: list[str], batch_df: DataFrame, batch_id: int) -> None:
    """foreachBatch handler. IDEMPOTENT: fail ke baad same batch_id dobara aa sakta hai ->
    MERGE (record_id) safe, appends txnAppId/txnVersion se safe (dobara nahi likhte)."""
    spark = batch_df.sparkSession  # serverless-safe (outer spark object use mat karo)
    if batch_df.isEmpty():
        return

    malformed = (
        batch_df.filter(F.col(CORRUPT_RECORD_COL).isNotNull())
        .withColumn("record_id", F.lit(None).cast("string"))
        .withColumn("_dq_errors", F.array(F.lit("malformed_csv_record")))
        .withColumn("_raw_record", F.col(CORRUPT_RECORD_COL))
    )
    cleaned = clean_members(batch_df.filter(F.col(CORRUPT_RECORD_COL).isNull()))
    valid, quarantine = apply_dq(cleaned, member_rules(regions))
    valid = valid.withColumn("dq_warning_count", F.size("_dq_warnings"))
    deduped = dedupe(valid)

    # MERGE: record_id content-hash hai -> matched = same data -> insert-only
    merge_upsert(
        spark,
        deduped.select(*SILVER_COLUMNS).withColumn("_silver_updated_ts", F.current_timestamp()),
        cfg.table("silver", "member_charges"), ["record_id"], change_condition="false",
    )

    quarantine_all = (
        quarantine.select(*QUARANTINE_COLUMNS).unionByName(malformed.select(*QUARANTINE_COLUMNS))
        .withColumn("_quarantined_ts", F.current_timestamp())
        .withColumn("_batch_id", F.lit(batch_id).cast("long"))
    )
    idem = {"txnAppId": "silver_member_charges", "txnVersion": str(batch_id)}
    write_table(quarantine_all, cfg.table("silver", "member_charges_quarantine"), "append", idem)

    stats = spark.createDataFrame(
        [(int(batch_id), batch_df.count(), valid.count(), deduped.count(), quarantine_all.count(),
          valid.filter("dq_warning_count > 0").count(), dt.datetime.now(dt.timezone.utc).replace(tzinfo=None))],
        "batch_id long, rows_in long, rows_valid long, rows_deduped long, rows_quarantined long, "
        "rows_with_warnings long, processed_ts timestamp",
    )
    write_table(stats, cfg.table("audit", "silver_batch_stats"), "append", idem)


def run_silver(spark: SparkSession, cfg: PipelineConfig) -> dict:
    # Reference data pehle - member DQ (region_in_master) isi par depend karta hai
    regions = run_region_master(spark, cfg)
    source = cfg.table("bronze", "member_charges_raw")
    if not table_exists(spark, source):
        raise RuntimeError("bronze.member_charges_raw nahi mili - pehle bronze chalao")

    start = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    query = (
        read_stream(spark.readStream, source)
        .writeStream.foreachBatch(lambda df, bid: _process_batch(cfg, regions, df, bid))
        .option("checkpointLocation", cfg.checkpoint("silver_member_charges"))
        .trigger(availableNow=True)
        .queryName("silver_member_charges")
        .start()
    )
    query.awaitTermination()

    totals = {"rows_in": 0, "rows_out": 0, "rows_quarantined": 0}
    stats_ref = cfg.table("audit", "silver_batch_stats")
    if table_exists(spark, stats_ref):
        r = (
            read_table(spark, stats_ref).filter(F.col("processed_ts") >= F.lit(start))
            .agg(F.sum("rows_in").alias("i"), F.sum("rows_deduped").alias("o"),
                 F.sum("rows_quarantined").alias("q"), F.sum("rows_valid").alias("v"))
            .first()
        )
        totals = {"rows_in": int(r.i or 0), "rows_out": int(r.o or 0), "rows_quarantined": int(r.q or 0)}
        log.info("SILVER: in=%d valid=%d duplicates_removed=%d quarantined=%d",
                 totals["rows_in"], r.v or 0, (r.v or 0) - totals["rows_out"], totals["rows_quarantined"])
    return {"layer": "silver", **totals}
