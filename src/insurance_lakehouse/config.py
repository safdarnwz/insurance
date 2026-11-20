"""
Config loader.

KYUN: Code ko environment (local / databricks) ka pata nahi hona chahiye. Code
sirf `cfg.table("silver", "member_charges")` poochta hai; config batata hai ki
wo local folder hai ya Unity Catalog table. Isse ek hi code laptop aur
Databricks par bina change ke chalta hai.

KYA NAHI KARNA: `if env == "prod": path = "..."` jaisi if-else poore code me mat
failao. Ek din koi ek jagah update karna bhool jayega aur galat jagah data likhega.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

# insurance-lakehouse/ folder (src/insurance_lakehouse/config.py se 2 level upar)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
# Config package ke ANDAR hai: Databricks par sirf src/ ka code chalta hai.
DEFAULT_CONFIG_FILE = Path(__file__).resolve().parent / "conf" / "config.yaml"

_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::([^}]*))?\}")


def _substitute_env(value: Any) -> Any:
    """YAML ki string values me ${VAR} / ${VAR:default} ko env variable se replace karta hai."""
    if isinstance(value, dict):
        return {k: _substitute_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_substitute_env(v) for v in value]
    if not isinstance(value, str):
        return value

    def repl(match: re.Match) -> str:
        var, default = match.group(1), match.group(2)
        resolved = os.environ.get(var, default)
        if resolved is None:
            # Fail fast: galat path par chupchaap data likhne se error better hai
            raise KeyError(f"Environment variable '{var}' set nahi hai (.env file dekho)")
        return resolved

    return _ENV_PATTERN.sub(repl, value)


@dataclass
class TableRef:
    """Ek Delta table ka pata: ya to path (local) ya UC naam (databricks)."""

    layer: str
    key: str
    path: str | None = None
    name: str | None = None

    @property
    def sql(self) -> str:
        """SQL me use karne layak identifier: catalog.schema.table ya delta.`/path`"""
        return self.name if self.name else f"delta.`{self.path}`"

    def __str__(self) -> str:
        return self.name or self.path or ""


@dataclass
class PipelineConfig:
    env: str
    raw: dict
    storage: dict = field(init=False)

    def __post_init__(self) -> None:
        self.env_cfg = self.raw["environments"][self.env]
        self.storage = {k: self._resolve_path(v) for k, v in self.env_cfg["storage"].items()}

    @staticmethod
    def _resolve_path(p: str) -> str:
        # /Volumes/... (Databricks) ya cloud URL waise hi
        if "://" in p or p.startswith("/Volumes/"):
            return p.rstrip("/")
        # Local relative path -> absolute (project root se). KYUN: Spark/Airflow
        # ka working dir alag ho sakta hai; relative path par table kahin aur ban jaayegi.
        path = Path(p)
        if not path.is_absolute():
            path = (PROJECT_ROOT / path).resolve()
        return path.as_posix()

    # --------------------------------------------------------------- basics
    @property
    def is_local(self) -> bool:
        return self.env == "local"

    @property
    def table_mode(self) -> str:
        return self.env_cfg.get("table_mode", "path")

    @property
    def catalog(self) -> str | None:
        return self.env_cfg.get("catalog")

    @property
    def schemas(self) -> dict:
        return self.raw["schemas"]

    @property
    def tables(self) -> dict:
        return self.raw["tables"]

    @property
    def sources(self) -> dict:
        return self.raw["sources"]

    @property
    def large_files(self) -> dict:
        return self.raw["large_files"]

    @property
    def quality_gate(self) -> dict:
        return self.raw["quality_gate"]

    @property
    def use_autoloader(self) -> bool:
        return bool(self.env_cfg.get("use_autoloader", False))

    @property
    def shuffle_partitions(self) -> str:
        return str(self.env_cfg.get("shuffle_partitions", "auto"))

    @property
    def dbt_target(self) -> str:
        return self.env_cfg.get("dbt_target", "local")

    @property
    def kaggle(self) -> dict:
        return self.raw["kaggle"]

    # --------------------------------------------------------------- tables
    def table(self, layer: str, key: str) -> TableRef:
        name = self.tables[layer][key]
        if self.table_mode == "catalog":
            return TableRef(layer, key, name=f"{self.catalog}.{self.schemas[layer]}.{name}")
        return TableRef(layer, key, path=f"{self.storage[layer]}/{name}")

    # --------------------------------------------------------------- files
    def landing_dir(self, source: str) -> str:
        """<landing>/<source_system>/<entity>  (iske neeche year=/month=/day=)"""
        s = self.sources[source]
        return f"{self.storage['landing']}/{s['source_system']}/{s['entity']}"

    def inbox_dir(self, source: str) -> str:
        """Drop zone: <inbox>/<source_system>/<entity>/  (source system yahan file rakhta hai)"""
        s = self.sources[source]
        return f"{self.storage['inbox']}/{s['source_system']}/{s['entity']}"

    def checkpoint(self, name: str) -> str:
        """Har streaming query ka apna checkpoint folder.
        KYA NAHI KARNA: do streams ka ek checkpoint share mat karo - offsets
        corrupt ho jaate hain aur data skip/duplicate hota hai."""
        return f"{self.storage['checkpoints']}/{name}"

    @property
    def warehouse_path(self) -> str | None:
        return self.storage.get("warehouse")


def _load_dotenv() -> None:
    """Simple .env loader. Pehle se set env vars override nahi hote -
    KYUN: Docker/Airflow me jo value set hai wahi jeetni chahiye."""
    env_file = PROJECT_ROOT / ".env"
    if not env_file.exists():
        return
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, val = line.split("=", 1)
        os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))


def load_config(env: str | None = None, config_file: str | Path | None = None) -> PipelineConfig:
    _load_dotenv()
    env = env or os.environ.get("PIPELINE_ENV", "local")
    config_file = Path(config_file or os.environ.get("PIPELINE_CONFIG", DEFAULT_CONFIG_FILE))
    with open(config_file, encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)  # safe_load: yaml.load arbitrary Python objects bana sakta hai
    envs = raw.get("environments", {})
    if env not in envs:
        raise ValueError(f"Unknown env '{env}'. Available: {list(envs)}")
    # Sirf selected env ke ${VAR} resolve karo
    raw["environments"] = {env: _substitute_env(envs[env])}
    return PipelineConfig(env=env, raw=raw)
