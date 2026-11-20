"""
Synthetic data: (1) next-year incremental batch, (2) bahut badi file (large-file demo).

KYUN chahiye:
  Kaggle file ek baar ka clean snapshot hai (1338 rows). Real company me data har
  din/saal aata hai, kabhi ganda bhi, aur kabhi GB/TB size me. Ye generator:
    1. Naye members ki file (same schema) -> incremental load + MERGE.
    2. Jaan-boojh kar ganda data -> DQ framework + quarantine kaam karte dikhein.
    3. Region master change (southeast office Atlanta -> Miami) -> dbt snapshot (SCD2).
    4. `generate_large_file` -> lakhon rows ki file -> auto-split + Spark partitions demo.

Charges ka formula original data jaisa pattern deta hai (age ke saath badhta,
smoker ~3x, smoker + obese sabse zyada). Sirf standard library; seed = reproducible.
"""

from __future__ import annotations

import csv
import random
import tempfile
from pathlib import Path

from ..config import PipelineConfig
from ..schemas import MEMBER_COLUMNS, REGION_COLUMNS
from ..utils.logger import get_logger
from .landing import SAMPLE_DIR, land_file

log = get_logger(__name__)

REGIONS = ["northeast", "northwest", "southeast", "southwest"]


def make_member(rng: random.Random) -> dict:
    age = rng.randint(18, 64)
    bmi = round(min(max(rng.gauss(30.6, 6.1), 16), 53), 2)
    smoker = rng.random() < 0.2
    children = rng.choices([0, 1, 2, 3, 4, 5], weights=[43, 24, 18, 12, 2, 1])[0]
    base = 260 * age - 2300 + 475 * children
    if smoker:
        base += 32000 if bmi >= 30 else 13000
    charges = max(base + rng.gauss(0, 2500) + rng.expovariate(1 / 1500), 1121.87)
    return {
        "age": str(age), "sex": rng.choice(["male", "female"]), "bmi": f"{bmi}",
        "children": str(children), "smoker": "yes" if smoker else "no",
        "region": rng.choice(REGIONS), "charges": f"{charges:.4f}",
    }


def _write_csv(path: Path, cols: list[str], rows: list[dict], extra_raw_lines: list[str] | None = None) -> None:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=cols)
        writer.writeheader()
        writer.writerows(rows)
        fh.writelines(line + "\n" for line in extra_raw_lines or [])


def _inject_dirty_data(rng: random.Random, rows: list[dict]) -> list[str]:
    """Real-world jaisi gandagi. Return: broken raw lines (corrupt record test)."""
    picks = rng.sample(range(len(rows)), 8)
    rows[picks[0]]["charges"] = "-1500.00"           # negative amount -> error
    rows[picks[1]]["age"] = "250"                     # impossible age -> error
    rows[picks[2]]["region"] = "central"              # region master me nahi -> error
    rows[picks[3]]["smoker"] = ""                     # mandatory missing -> error
    rows[picks[4]]["bmi"] = "abc"                     # non-numeric -> error
    rows[picks[5]]["charges"] = "185000.00"           # outlier -> sirf WARN
    rows[picks[6]]["sex"] = " FEMALE "                # formatting -> silver me fix
    rows[picks[7]]["region"] = "SouthEast"            # case -> silver me fix
    clean_idx = [i for i in range(len(rows)) if i not in set(picks)]
    rows.extend(dict(rows[i]) for i in rng.sample(clean_idx, 3))  # 3 exact duplicates
    return ['33,"male,unclosed quote line,no']        # broken CSV line


def generate_incremental_batch(cfg: PipelineConfig, batch_no: int, ingest_date: str, n_new: int = 200) -> dict:
    rng = random.Random(1000 + batch_no)
    rows = [make_member(rng) for _ in range(n_new)]
    broken = _inject_dirty_data(rng, rows)
    with tempfile.TemporaryDirectory() as tmp:
        src = Path(tmp) / "members.csv"
        _write_csv(src, MEMBER_COLUMNS, rows, broken)
        manifest = land_file(cfg, "member_charges", src, ingest_date)

        if batch_no == 2:  # Region master change -> SCD2 demo
            with open(SAMPLE_DIR / "region_master.csv", newline="", encoding="utf-8") as fh:
                regions = list(csv.DictReader(fh))
            for r in regions:
                if r["region"] == "southeast":
                    r["regional_office"] = "Miami"
                if r["region"] == "northwest":
                    r["risk_zone"] = "Medium"
            rsrc = Path(tmp) / "regions.csv"
            _write_csv(rsrc, REGION_COLUMNS, regions)
            land_file(cfg, "region_master", rsrc, ingest_date)
            log.info("Region master change: southeast office -> Miami, northwest risk -> Medium")
    log.info("Incremental batch %d: %d rows (dirty rows + duplicates ke saath)", batch_no, manifest["total_rows"])
    return manifest


def generate_large_file(output: str | Path, rows: int, seed: int = 7) -> Path:
    """Bada CSV banao (default 2M rows ~ 100 MB) - split + partitioning demo ke liye."""
    rng = random.Random(seed)
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with open(output, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(MEMBER_COLUMNS)
        for _ in range(rows):
            m = make_member(rng)
            w.writerow([m[c] for c in MEMBER_COLUMNS])
    log.info("Large file: %s (%d rows, %.1f MB)", output, rows, output.stat().st_size / 1024 / 1024)
    return output
