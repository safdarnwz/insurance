"""
Source file -> LANDING zone, company-standard naming + manifest + auto-split.

LANDING PATH STANDARD  (docs/FILE_NAMING.md me poori explanation)
------------------------------------------------------------------
  <landing>/<source_system>/<entity>/year=YYYY/month=MM/day=DD/
      <source_system>_<entity>_<YYYYMMDDTHHMMSSZ>_<batch_id>_part-00001.csv
      _manifest_<source_system>_<entity>_<YYYYMMDDTHHMMSSZ>_<batch_id>.json

  example:
  landing/kaggle/member_charges/year=2026/month=01/day=01/
      kaggle_member_charges_20260101T020000Z_a1b2c3d4_part-00001.csv

  * year=/month=/day=  -> business date (kis din ka data hai). Hive-style
    partition: Spark khud year/month/day columns bana leta hai, aur
    "January 2026 ka data" = sirf ek folder padhna (partition pruning).
  * timestamp (UTC, ISO-8601 basic) -> kab extract hua. Sortable, timezone confusion nahi.
  * batch_id -> ek run ki saari files ek saath pehchaan me aayein (rerun / debug).
  * part-NNNNN -> badi file ke tukde (zero-padded taaki sort sahi ho).
  * manifest JSON -> har part ki row count + size + sha256. Bronze ke baad
    "reconcile" step check karta hai ki jitni rows bheji gayi utni hi load hui.
    Underscore se start -> Spark use data file nahi maanta (hidden file).

KYA NAHI KARNA: "final_data_v2_NEW (1).csv" jaise naam, spaces, local timezone,
DD-MM-YYYY (sort nahi hota), ek hi naam se file overwrite karna.

LANDING kyun alag (seedha bronze me nahi): source ki original file proof ke
liye safe rehti hai, bronze kabhi bhi dobara bana sakte ho (replay).
"""

from __future__ import annotations

import datetime as dt
import hashlib
import io
import json
import os
import shutil
import tempfile
import uuid
from pathlib import Path

from ..config import PROJECT_ROOT, PipelineConfig
from ..utils.logger import get_logger

log = get_logger(__name__)

SAMPLE_DIR = PROJECT_ROOT / "sample_data"
MB = 1024 * 1024


# =============================================================== naming
def partition_path(cfg: PipelineConfig, source: str, ingest_date: str) -> str:
    d = dt.date.fromisoformat(ingest_date)  # galat format par yahin fail (fail fast)
    return f"{cfg.landing_dir(source)}/year={d.year:04d}/month={d.month:02d}/day={d.day:02d}"


def file_prefix(cfg: PipelineConfig, source: str, extract_ts: dt.datetime, batch_id: str) -> str:
    s = cfg.sources[source]
    return f"{s['source_system']}_{s['entity']}_{extract_ts.strftime('%Y%m%dT%H%M%SZ')}_{batch_id}"


def new_batch_id() -> str:
    return uuid.uuid4().hex[:8]


# =============================================================== split
def split_csv(src: Path, out_dir: Path, prefix: str, chunk_mb: int) -> list[dict]:
    """Badi CSV ko ~chunk_mb ke parts me todta hai. Har part me header repeat.

    LINE-level split (bytes), CSV re-parse nahi - KYUN: landing me data bilkul
    as-is rehna chahiye; parse+rewrite karne se quoting/kharab lines badal jaati hain.
    ASSUMPTION: quoted field ke andar newline nahi hai. Agar source me multi-line
    fields hain to source se hi Parquet / pre-split files maango.

    Memory: line-by-line stream -> 1 TB file bhi constant memory me split hoti hai.
    """
    chunk_bytes = chunk_mb * MB
    parts: list[dict] = []
    with open(src, "rb") as fh:
        header = fh.readline()
        part_no, out, written, rows, sha = 0, None, 0, 0, None

        def close_part():
            if out:
                out.close()
                parts[-1].update(rows=rows, bytes=written + len(header), sha256=sha.hexdigest())

        for line in fh:
            if not line.strip():
                continue  # khali lines count/data me nahi
            if out is None or written >= chunk_bytes:
                close_part()
                part_no += 1
                name = f"{prefix}_part-{part_no:05d}.csv"
                out = open(out_dir / name, "wb")
                out.write(header)
                sha = hashlib.sha256(header)
                parts.append({"file": name})
                written, rows = 0, 0
            out.write(line)
            sha.update(line)
            written += len(line)
            rows += 1
        if out is None:  # sirf header wali file
            name = f"{prefix}_part-00001.csv"
            (out_dir / name).write_bytes(header)
            parts.append({"file": name, "rows": 0, "bytes": len(header),
                          "sha256": hashlib.sha256(header).hexdigest()})
        else:
            close_part()
    return parts


# =============================================================== writers
def _is_volume(path: str) -> bool:
    return path.startswith("/Volumes/")


def _upload(local_file: Path, target_dir: str) -> None:
    """Local folder ya Databricks UC Volume me file rakho."""
    if _is_volume(target_dir) and "DATABRICKS_RUNTIME_VERSION" not in os.environ:
        # Laptop / Airflow container se Databricks Volume me upload (Files API).
        # Auth: DATABRICKS_HOST + DATABRICKS_TOKEN env (.env file) - code me kabhi nahi.
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        w.files.create_directory(target_dir)
        with open(local_file, "rb") as fh:
            w.files.upload(f"{target_dir}/{local_file.name}", io.BytesIO(fh.read()), overwrite=True)
        return
    Path(target_dir).mkdir(parents=True, exist_ok=True)
    shutil.copyfile(local_file, Path(target_dir) / local_file.name)


def land_file(cfg: PipelineConfig, source: str, src: str | Path, ingest_date: str,
              batch_id: str | None = None, split_threshold_mb: int | None = None,
              chunk_mb: int | None = None) -> dict:
    """Ek source file ko landing me rakho (zaroorat ho to split karke) + manifest likho."""
    src = Path(src)
    if not src.exists():
        raise FileNotFoundError(src)
    lf = cfg.large_files
    threshold = (split_threshold_mb or lf["split_threshold_mb"]) * MB
    chunk = chunk_mb or lf["split_chunk_mb"]
    batch_id = batch_id or new_batch_id()
    extract_ts = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    prefix = file_prefix(cfg, source, extract_ts, batch_id)
    target_dir = partition_path(cfg, source, ingest_date)
    size = src.stat().st_size

    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        # Chhoti file -> 1 part; badi file -> chunk_mb ke parts
        parts = split_csv(src, tmp_dir, prefix, chunk if size > threshold else max(1, size // MB + 1))
        if len(parts) > 1:
            log.info("Badi file (%.1f MB) -> %d parts of ~%d MB", size / MB, len(parts), chunk)
        manifest = {
            "source_system": cfg.sources[source]["source_system"],
            "entity": cfg.sources[source]["entity"],
            "business_date": ingest_date,
            "extract_ts_utc": extract_ts.isoformat(),
            "batch_id": batch_id,
            "original_file": src.name,
            "original_bytes": size,
            "total_rows": sum(p["rows"] for p in parts),
            "parts": parts,
        }
        manifest_file = tmp_dir / f"_manifest_{prefix}.json"
        manifest_file.write_text(json.dumps(manifest, indent=2))
        # Pehle data files, LAST me manifest. KYUN: downstream manifest dekh kar
        # samjhe ki batch complete hai (adhoori upload process na ho).
        for p in parts:
            _upload(tmp_dir / p["file"], target_dir)
        _upload(manifest_file, target_dir)

    log.info("Landed %s: %d rows, %d file(s) -> %s", source, manifest["total_rows"], len(parts), target_dir)
    return manifest


# =============================================================== sources
def kaggle_credentials_available() -> bool:
    if os.environ.get("KAGGLE_USERNAME") and os.environ.get("KAGGLE_KEY"):
        return True
    kaggle_dir = Path(os.environ.get("KAGGLE_CONFIG_DIR", Path.home() / ".kaggle"))
    return (kaggle_dir / "kaggle.json").exists()


def land_from_kaggle(cfg: PipelineConfig, ingest_date: str, batch_id: str | None = None) -> dict:
    """Kaggle API se download -> landing.
    KYA NAHI KARNA: kaggle.json git me commit mat karna."""
    from kaggle.api.kaggle_api_extended import KaggleApi  # import yahin: bina creds ke baaki code chale

    api = KaggleApi()
    api.authenticate()
    with tempfile.TemporaryDirectory() as tmp:
        api.dataset_download_files(cfg.kaggle["dataset"], path=tmp, unzip=True, quiet=True)
        return land_file(cfg, "member_charges", Path(tmp) / cfg.kaggle["file_name"], ingest_date, batch_id)


def land_region_master(cfg: PipelineConfig, ingest_date: str, src: Path | None = None,
                       batch_id: str | None = None) -> dict:
    """Reference data. Real company me CRM/ERP se aata hai."""
    return land_file(cfg, "region_master", src or SAMPLE_DIR / "region_master.csv", ingest_date, batch_id)


# =============================================================== drop zone (inbox)
def _is_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def upload_to_inbox(cfg: PipelineConfig, source: str, src: str | Path) -> str:
    """Source system ki tarah file inbox (drop zone) me rakho.
    Databricks par ye kaam aap UI se bhi kar sakte ho: Catalog -> workspace ->
    insurance_landing -> inbox volume -> Upload to this volume."""
    src = Path(src)
    target_dir = cfg.inbox_dir(source)
    _upload(src, target_dir)
    log.info("Inbox me file rakhi: %s/%s", target_dir, src.name)
    return f"{target_dir}/{src.name}"


def land_from_inbox(cfg: PipelineConfig, ingest_date: str) -> list[dict]:
    """INBOX (drop zone) -> LANDING (standard naming + manifest) -> original _archive me.

    Industry pattern (SFTP/drop-zone): source team kisi bhi naam se file daalti hai
    ("Insurance Data FINAL.csv"); pipeline use standard naam deta hai, manifest
    likhta hai, aur original ko _archive me move karta hai -> agli run me dobara
    process nahi hoti (idempotent), aur original proof bhi safe rehta hai.

    Databricks job ke andar /Volumes normal folder ki tarah dikhta hai (FUSE),
    isliye yahi code laptop aur Databricks dono par chalta hai.
    """
    if _is_volume(cfg.storage["inbox"]) and not _is_databricks():
        raise RuntimeError("Volume inbox ko Databricks job ke andar process karo (bundle task `ingest`)")
    manifests = []
    for source in cfg.sources:
        inbox = Path(cfg.inbox_dir(source))
        files = sorted(p for p in inbox.glob("*.csv") if p.is_file()) if inbox.exists() else []
        if not files:
            log.info("INBOX %s: koi nayi file nahi", source)
            continue
        archive = Path(cfg.storage["inbox"]) / "_archive" / cfg.sources[source]["source_system"] \
            / cfg.sources[source]["entity"] / ingest_date
        archive.mkdir(parents=True, exist_ok=True)
        for f in files:
            manifests.append(land_file(cfg, source, f, ingest_date))
            # Move SIRF successful landing ke baad - fail hua to file inbox me rahegi, retry par dobara
            shutil.move(str(f), str(archive / f.name))
            log.info("INBOX %s: %s -> landed + archived", source, f.name)
    return manifests
