"""
Delta Lake helpers - local (path) aur Databricks (Unity Catalog name) dono ke liye ek API.

Delta Lake KYUN (plain CSV/Parquet ki jagah):
  * ACID transactions  -> job beech me fail ho to aadha-adhoora data nahi dikhta
  * MERGE (upsert)      -> incremental loads, idempotent reruns
  * Schema enforcement  -> galat column/type wala data table me nahi ghus sakta
  * Time travel         -> `VERSION AS OF 5` se purana data dekh/restore
  * OPTIMIZE / Z-ORDER  -> small files compact, fast queries

KYUN Databricks par UC managed tables (path ki jagah):
  Unity Catalog permissions, lineage, audit sab table naam par kaam karte hain.
  Path-based tables UC governance ko bypass karti hain.
"""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import DataStreamReader, DataStreamWriter

from ..config import TableRef
from .logger import get_logger

log = get_logger(__name__)


def table_exists(spark: SparkSession, ref: TableRef) -> bool:
    if ref.name:
        return spark.catalog.tableExists(ref.name)
    return DeltaTable.isDeltaTable(spark, ref.path)


def read_table(spark: SparkSession, ref: TableRef) -> DataFrame:
    return spark.read.table(ref.name) if ref.name else spark.read.format("delta").load(ref.path)


def read_stream(reader: DataStreamReader, ref: TableRef) -> DataFrame:
    return reader.table(ref.name) if ref.name else reader.format("delta").load(ref.path)


def delta_table(spark: SparkSession, ref: TableRef) -> DeltaTable:
    return DeltaTable.forName(spark, ref.name) if ref.name else DeltaTable.forPath(spark, ref.path)


def write_table(df: DataFrame, ref: TableRef, mode: str = "append", options: dict | None = None,
                partition_by: list[str] | None = None) -> None:
    writer = df.write.format("delta").mode(mode)
    for k, v in (options or {}).items():
        writer = writer.option(k, v)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    if ref.name:
        writer.saveAsTable(ref.name)
    else:
        writer.save(ref.path)


def start_stream(writer: DataStreamWriter, ref: TableRef):
    return writer.toTable(ref.name) if ref.name else writer.start(ref.path)


def last_operation_metrics(spark: SparkSession, ref: TableRef) -> dict:
    row = delta_table(spark, ref).history(1).select("operationMetrics").collect()[0][0]
    return dict(row or {})


def merge_upsert(spark: SparkSession, df: DataFrame, ref: TableRef, key_cols: list[str],
                 change_condition: str | None = None, update_set: dict | None = None,
                 partition_by: list[str] | None = None) -> None:
    """Generic SCD Type-1 upsert (MERGE).

    - Table nahi hai -> pehli baar create (initial load)
    - Key match + change_condition true -> UPDATE
    - Key nahi mili -> INSERT

    change_condition: e.g. "s._row_hash <> t._row_hash" - KYUN: bina condition ke
    MERGE har matched row ko dobara likhta hai (same data par bhi) -> bekaar files,
    slow. "false" = insert-only (matched rows ko kabhi touch mat karo).
    """
    if not table_exists(spark, ref):
        log.info("%s exist nahi karti - initial load", ref)
        write_table(df, ref, mode="overwrite", partition_by=partition_by)
        return

    on = " AND ".join(f"t.{c} = s.{c}" for c in key_cols)
    builder = delta_table(spark, ref).alias("t").merge(df.alias("s"), on)
    if update_set:
        builder = builder.whenMatchedUpdate(condition=change_condition, set=update_set)
    elif change_condition != "false":
        builder = builder.whenMatchedUpdateAll(condition=change_condition)
    builder.whenNotMatchedInsertAll().execute()

    m = last_operation_metrics(spark, ref)
    log.info("MERGE %s -> inserted=%s updated=%s", ref, m.get("numTargetRowsInserted"), m.get("numTargetRowsUpdated"))
