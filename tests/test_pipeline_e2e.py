"""End-to-end: landing -> bronze -> reconcile -> silver -> dq gate -> gold (dbt + DuckDB).

Temp folder me chalta hai (asli ./data ko touch nahi karta). Check karta hai:
counts, DQ quarantine, reconciliation, dbt tests, SCD2 point-in-time, idempotency.
"""

import shutil

import pytest

from insurance_lakehouse.ingestion.landing import SAMPLE_DIR, land_file, land_region_master
from insurance_lakehouse.ingestion.synthetic_data import generate_incremental_batch
from insurance_lakehouse.pipeline import run_layers
from insurance_lakehouse.quality.gate import dq_gate
from insurance_lakehouse.quality.reconcile import reconcile
from insurance_lakehouse.utils.delta_utils import read_table

SPARK_LAYERS = ["bronze", "silver"]


def _count(spark, cfg, layer, key, where=None):
    df = read_table(spark, cfg.table(layer, key))
    return df.filter(where).count() if where else df.count()


HAS_DBT = shutil.which("dbt") is not None


def _gold(spark, cfg):
    """Har load ke baad gold - jaise Airflow DAG karta hai.
    ZAROORI: dbt snapshot sirf wahi history pakadta hai jo har run me dekhta hai.
    Do region changes ke beech gold na chale to beech wala version kabhi record nahi hoga."""
    if HAS_DBT:
        run_layers(spark, cfg, ["gold"])


def test_end_to_end(spark, cfg):
    # ---- Initial load: Kaggle file (1338 rows, 1 exact duplicate)
    land_file(cfg, "member_charges", SAMPLE_DIR / "insurance.csv", "2026-01-01")
    land_region_master(cfg, "2026-01-01")
    bronze, silver = run_layers(spark, cfg, SPARK_LAYERS)
    assert bronze["rows_in"] == 1338 + 4
    assert silver["rows_out"] == 1337 and silver["rows_quarantined"] == 0
    reconcile(spark, cfg)
    assert dq_gate(spark, cfg) == 0.0
    _gold(spark, cfg)

    # ---- Next year: 200 new + 3 duplicates + 5 bad rows + 1 broken line + region change
    generate_incremental_batch(cfg, batch_no=2, ingest_date="2027-01-01")
    _, silver = run_layers(spark, cfg, SPARK_LAYERS)
    assert silver["rows_in"] == 204 and silver["rows_quarantined"] == 6 and silver["rows_out"] == 195
    assert _count(spark, cfg, "silver", "member_charges") == 1337 + 195
    reconcile(spark, cfg)
    assert 0 < dq_gate(spark, cfg) < 5
    _gold(spark, cfg)

    # Gate strict ho to pipeline rukni chahiye
    with pytest.raises(RuntimeError, match="DQ GATE FAILED"):
        dq_gate(spark, cfg, max_reject_pct=1.0)

    # ---- Rerun without new data: idempotent
    r = run_layers(spark, cfg, SPARK_LAYERS)
    assert r[0]["rows_in"] == 0 and r[1]["rows_in"] == 0
    assert _count(spark, cfg, "silver", "member_charges") == 1532


@pytest.mark.skipif(not HAS_DBT, reason="dbt installed nahi")
def test_gold_dbt(spark, cfg):
    """test_end_to_end ke baad (same session data): gold ki checks + rerun idempotency."""
    import duckdb

    g = cfg.schemas["gold"]
    with duckdb.connect(cfg.warehouse_path, read_only=True) as con:
        assert con.sql(f"select count(*) from {g}.fact_member_charges").fetchone()[0] == 1532
        # SCD2: 2 regions badle -> 2 expired versions
        assert con.sql(f"select count(*) from {g}.dim_region where not is_current").fetchone()[0] == 2
        # Point-in-time: 2026 ke southeast records Atlanta se, 2027 wale Miami se
        offices = dict(con.sql(f"""
            select f.policy_year, any_value(r.regional_office)
            from {g}.fact_member_charges f join {g}.dim_region r using (region_sk)
            where r.region = 'southeast' group by 1""").fetchall())
        assert offices == {2026: "Atlanta", 2027: "Miami"}

    # Rerun gold: incremental + snapshot -> kuch nahi badalna chahiye
    run_layers(spark, cfg, ["gold"])
    with duckdb.connect(cfg.warehouse_path, read_only=True) as con:
        assert con.sql(f"select count(*) from {g}.fact_member_charges").fetchone()[0] == 1532
        assert con.sql(f"select count(*) from {g}.dim_region").fetchone()[0] == 6
