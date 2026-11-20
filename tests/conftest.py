"""
Pytest fixtures.

KYUN tests: Data pipelines me bug chupchaap galat numbers banata hai (crash
nahi hota) - CFO ki report galat. Unit tests transformation logic ko chhote,
known data par check karte hain. CI (GitHub Actions) har PR par chalata hai.

Session-scoped SparkSession: Spark start hone me ~5-10 sec lagte hain, har
test me naya banana bahut slow hoga.
"""

import pytest

from insurance_lakehouse.config import load_config


@pytest.fixture(scope="session")
def cfg(tmp_path_factory):
    c = load_config("local")
    base = tmp_path_factory.mktemp("lake").as_posix()
    # Tests apne temp folder me likhte hain - asli ./data ko kabhi touch nahi karte
    c.storage = {k: f"{base}/{k}" for k in c.storage}
    return c


@pytest.fixture(scope="session")
def spark(cfg):
    from insurance_lakehouse.spark_session import get_spark

    s = get_spark(cfg, app_name="insurance-lakehouse-tests")
    yield s
    s.stop()
