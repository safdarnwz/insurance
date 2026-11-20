"""
CLI entry point  (`pip install -e .` ke baad `insurance-pipeline` command).

  insurance-pipeline setup                                   # folders / UC schemas+volumes
  insurance-pipeline ingest --ingest-date 2026-01-01         # sample_data/insurance.csv -> landing
  insurance-pipeline ingest --source kaggle                  # Kaggle API se
  insurance-pipeline ingest --source synthetic --batch 2 --ingest-date 2027-01-01   # next year + dirty data
  insurance-pipeline generate-large --rows 2000000           # ~100 MB file (large-file demo)
  insurance-pipeline ingest --source file --path data/tmp/large.csv --split-mb 16 --chunk-mb 16
  insurance-pipeline run --layer bronze|silver|gold|all
  insurance-pipeline reconcile                               # manifest rows == bronze rows?
  insurance-pipeline dq-gate                                 # reject% > limit -> fail
  insurance-pipeline dbt docs generate                       # koi bhi dbt command
  insurance-pipeline show                                    # audit + gold tables
  insurance-pipeline --env databricks ...                    # Databricks Free Edition

KYUN CLI: same command laptop, Docker/Airflow, CI aur Databricks job - sab jagah.
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import shutil
import sys
from pathlib import Path

from .config import load_config
from .utils.logger import get_logger

log = get_logger("insurance_lakehouse")


def _spark(cfg):
    from .spark_session import get_spark

    return get_spark(cfg)


def _cmd_setup(cfg, args):
    from .utils.maintenance import setup

    setup(_spark(cfg) if cfg.table_mode == "catalog" else None, cfg)


def _cmd_ingest(cfg, args):
    from .ingestion import landing
    from .ingestion.synthetic_data import generate_incremental_batch

    d = args.ingest_date or dt.date.today().isoformat()
    if args.source == "synthetic":
        generate_incremental_batch(cfg, args.batch, ingest_date=d)
        return
    if args.source == "kaggle":
        if not landing.kaggle_credentials_available():
            raise SystemExit("Kaggle credentials nahi mile (~/.kaggle/kaggle.json ya KAGGLE_USERNAME/KAGGLE_KEY)")
        landing.land_from_kaggle(cfg, d)
    else:
        src = args.path if args.source == "file" else landing.SAMPLE_DIR / "insurance.csv"
        if not src:
            raise SystemExit("--source file ke saath --path <csv> do")
        landing.land_file(cfg, "member_charges", src, d, split_threshold_mb=args.split_mb, chunk_mb=args.chunk_mb)
    # Reference data sirf initial load (sample/kaggle) ke saath. KYUN: purani
    # region_master.csv dobara bhejne se baad ke changes (Miami) revert ho jaate.
    # Real me reference data apne source system se apne schedule par aata hai.
    if args.source in ("sample", "kaggle") or args.with_region_master:
        landing.land_region_master(cfg, d)


def _cmd_generate_large(cfg, args):
    from .ingestion.synthetic_data import generate_large_file

    generate_large_file(args.output, args.rows)


def _cmd_run(cfg, args):
    from .pipeline import run_layers

    layers = ["bronze", "silver", "gold"] if args.layer == "all" else [args.layer]
    # Gold (dbt) ko Spark nahi chahiye - Databricks env me Airflow container se
    # sirf dbt chalta hai, local Spark start karna bekaar hai.
    spark = None if layers == ["gold"] and not cfg.is_local else _spark(cfg)
    for r in run_layers(spark, cfg, layers):
        log.info("RESULT %s", r)


def _cmd_reconcile(cfg, args):
    from .quality.reconcile import reconcile

    reconcile(_spark(cfg), cfg)


def _cmd_dq_gate(cfg, args):
    from .quality.gate import dq_gate

    dq_gate(_spark(cfg), cfg, args.max_reject_pct)


def _cmd_dbt(cfg, args):
    from .gold.dbt_runner import run_dbt

    run_dbt(cfg, args.dbt_args)


def _cmd_maintenance(cfg, args):
    from .utils.maintenance import run_maintenance

    run_maintenance(_spark(cfg), cfg, retain_hours=args.retain_hours)


def _cmd_show(cfg, args):
    from .utils.delta_utils import read_table, table_exists

    spark = _spark(cfg)
    for layer, key, order in [("audit", "pipeline_runs", "start_ts"),
                              ("silver", "member_charges_quarantine", None)]:
        ref = cfg.table(layer, key)
        if table_exists(spark, ref):
            df = read_table(spark, ref)
            print(f"\n===== {layer}.{key} ({df.count()} rows) =====")
            (df.orderBy(order) if order else df).drop("_raw_record").show(30, truncate=60)

    if not cfg.is_local:
        print("\nGold tables Databricks SQL editor me dekho:", f"{cfg.catalog}.{cfg.schemas['gold']}.*")
        return
    import duckdb

    if not Path(cfg.warehouse_path).exists():
        print("\nDuckDB warehouse abhi nahi bana - `run --layer gold` chalao")
        return
    con = duckdb.connect(cfg.warehouse_path, read_only=True)
    g = cfg.schemas["gold"]
    for sql in [
        f"select * from {g}.agg_region_risk order by region",
        f"select * from {g}.agg_smoker_loading order by age_band",
        f"select region, regional_office, risk_zone, effective_from, effective_to, is_current "
        f"from {g}.dim_region order by region, effective_from",
        f"select policy_year, count(*) members, round(sum(charges),2) total_charges "
        f"from {g}.fact_member_charges group by 1 order by 1",
    ]:
        print(f"\n===== {sql.split(' from ')[1].split()[0]} =====")
        print(con.sql(sql))


def _cmd_reset(cfg, args):
    """Local data saaf (fresh start). Databricks par jaan-boojh kar allowed nahi."""
    if not cfg.is_local:
        raise SystemExit("reset sirf local env me allowed hai")
    data_dir = Path(cfg.storage["landing"]).parent  # ./data
    if not args.yes and input(f"{data_dir} delete karna hai? (yes/no): ").strip().lower() != "yes":
        return
    shutil.rmtree(data_dir, ignore_errors=True)
    log.info("Deleted %s", data_dir)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="insurance-pipeline", description="Insurance Medallion Lakehouse (ELT)")
    p.add_argument("--env", default=None, help="local | databricks (default: PIPELINE_ENV ya local)")
    p.add_argument("--catalog", default=None, help="Databricks UC catalog (default: workspace)")
    sub = p.add_subparsers(dest="command", required=True)

    sub.add_parser("setup", help="Folders / UC schemas + volumes banao")

    s = sub.add_parser("ingest", help="Source file -> landing (standard naming + manifest)")
    s.add_argument("--source", choices=["sample", "kaggle", "file", "synthetic"], default="sample")
    s.add_argument("--path", default=None, help="--source file ke liye CSV path")
    s.add_argument("--batch", type=int, default=2, help="--source synthetic: batch number")
    s.add_argument("--ingest-date", default=None, help="Business date YYYY-MM-DD (default: aaj)")
    s.add_argument("--split-mb", type=int, default=None, help="Isse badi file split ho (default config)")
    s.add_argument("--chunk-mb", type=int, default=None, help="Har part ka size MB (default config)")
    s.add_argument("--with-region-master", action="store_true", help="sample_data/region_master.csv bhi bhejo")

    s = sub.add_parser("generate-large", help="Bada synthetic CSV (large-file demo)")
    s.add_argument("--rows", type=int, default=2_000_000)
    s.add_argument("--output", default="data/tmp/large_member_charges.csv")

    s = sub.add_parser("run", help="Layers chalao")
    s.add_argument("--layer", choices=["bronze", "silver", "gold", "all"], default="all")

    sub.add_parser("reconcile", help="Landing manifest vs bronze row counts")
    s = sub.add_parser("dq-gate", help="Reject % check (circuit breaker)")
    s.add_argument("--max-reject-pct", type=float, default=None)

    s = sub.add_parser("dbt", help="dbt command chalao, e.g. `dbt docs generate`")
    s.add_argument("dbt_args", nargs=argparse.REMAINDER)

    s = sub.add_parser("maintenance", help="OPTIMIZE + VACUUM")
    s.add_argument("--retain-hours", type=int, default=168)

    sub.add_parser("show", help="Audit + gold tables print karo")
    s = sub.add_parser("reset", help="Local data delete")
    s.add_argument("--yes", action="store_true")
    return p


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(sys.argv[1:] if argv is None else argv)
    if args.catalog:
        os.environ["UC_CATALOG"] = args.catalog  # config.yaml ${UC_CATALOG:workspace}
    cfg = load_config(args.env)
    log.info("env=%s command=%s", cfg.env, args.command)
    {
        "setup": _cmd_setup, "ingest": _cmd_ingest, "generate-large": _cmd_generate_large,
        "run": _cmd_run, "reconcile": _cmd_reconcile, "dq-gate": _cmd_dq_gate, "dbt": _cmd_dbt,
        "maintenance": _cmd_maintenance, "show": _cmd_show, "reset": _cmd_reset,
    }[args.command](cfg, args)


if __name__ == "__main__":
    main()
