"""Landing zone: naming standard, large-file split, manifest (pure Python, Spark nahi)."""

import copy
import json
import re
from pathlib import Path

import pytest

from insurance_lakehouse.ingestion.landing import land_file, partition_path, split_csv

NAME_RE = re.compile(r"^kaggle_member_charges_\d{8}T\d{6}Z_[0-9a-f]{8}_part-\d{5}\.csv$")


@pytest.fixture
def lcfg(cfg, tmp_path):
    """Apna alag landing folder - e2e test ke data me mix na ho."""
    c = copy.copy(cfg)
    c.storage = {**cfg.storage, "landing": (tmp_path / "landing").as_posix()}
    return c


def _csv(path: Path, rows: int) -> Path:
    lines = ["age,sex,bmi,children,smoker,region,charges"]
    lines += [f"{20 + i % 40},male,30.1,0,no,southeast,{1000 + i}.5" for i in range(rows)]
    path.write_text("\n".join(lines) + "\n")
    return path


def test_partition_path_is_hive_style(lcfg):
    p = partition_path(lcfg, "member_charges", "2027-03-05")
    assert p.endswith("/kaggle/member_charges/year=2027/month=03/day=05")


def test_split_keeps_header_and_all_rows(tmp_path):
    src = _csv(tmp_path / "big.csv", 50_000)
    out = tmp_path / "out"
    out.mkdir()
    parts = split_csv(src, out, "kaggle_member_charges_20270101T000000Z_abcd1234", chunk_mb=1)
    assert len(parts) > 1                                   # sach me split hua
    assert sum(p["rows"] for p in parts) == 50_000          # koi row lost/duplicate nahi
    for p in parts:
        lines = (out / p["file"]).read_text().splitlines()
        assert lines[0].startswith("age,sex")               # har part me header
        assert len(lines) - 1 == p["rows"]
    assert [p["file"] for p in parts] == sorted(p["file"] for p in parts)  # zero-padded -> sort sahi


def test_land_file_naming_and_manifest(lcfg, tmp_path):
    src = _csv(tmp_path / "Insurance Data FINAL (1).csv", 1000)   # ganda naam -> standard naam
    m = land_file(lcfg, "member_charges", src, "2027-01-01", split_threshold_mb=0, chunk_mb=1)
    folder = Path(partition_path(lcfg, "member_charges", "2027-01-01"))
    files = sorted(f.name for f in folder.glob("*.csv"))
    assert files and all(NAME_RE.match(f) for f in files)
    manifest = json.loads(next(folder.glob("_manifest_*.json")).read_text())
    assert manifest["total_rows"] == 1000 == m["total_rows"]
    assert manifest["original_file"] == "Insurance Data FINAL (1).csv"
    assert len(manifest["parts"][0]["sha256"]) == 64
