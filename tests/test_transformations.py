"""Silver transformation + DQ unit tests (chhota in-memory data, koi file I/O nahi)."""

import datetime as dt
from decimal import Decimal

from insurance_lakehouse.quality.dq_checks import apply_dq, member_rules
from insurance_lakehouse.schemas import MEMBER_COLUMNS
from insurance_lakehouse.silver.transform_silver import clean_members, dedupe

REGIONS = ["northeast", "northwest", "southeast", "southwest"]
META = ["_source_file", "_source_file_ts", "_ingest_ts", "_landing_date"]


def _row(**kw):
    r = {"age": "19", "sex": "female", "bmi": "27.9", "children": "0", "smoker": "yes",
         "region": "southwest", "charges": "16884.924",
         "_source_file": "f1.csv", "_source_file_ts": dt.datetime(2026, 1, 1),
         "_ingest_ts": dt.datetime(2026, 1, 1), "_landing_date": dt.date(2026, 1, 1)}
    r.update(kw)
    return r


def _df(spark, rows):
    cols = MEMBER_COLUMNS + META
    types = {"_source_file_ts": "timestamp", "_ingest_ts": "timestamp", "_landing_date": "date"}
    schema = ", ".join(f"{c} {types.get(c, 'string')}" for c in cols)
    return spark.createDataFrame([tuple(r[c] for c in cols) for r in rows], schema)


def test_clean_types_and_derivations(spark):
    out = clean_members(_df(spark, [_row(sex=" FEMALE ", region="SouthWest")])).first()
    assert out.sex == "female" and out.region == "southwest"      # standardize
    assert out.age == 19 and out.bmi == Decimal("27.90")
    assert out.charges == Decimal("16884.92")                     # DECIMAL, double nahi
    assert out.is_smoker is True
    assert out.age_band == "18-24"
    assert out.bmi_category == "Overweight"
    assert out.policy_year == 2026
    assert out.record_id is not None and len(out.record_id) == 64


def test_bmi_categories(spark):
    rows = [_row(bmi=b, charges=str(i)) for i, b in enumerate(["17", "22", "27", "35"], start=1)]
    cats = [r.bmi_category for r in clean_members(_df(spark, rows)).orderBy("charges").collect()]
    assert cats == ["Underweight", "Normal", "Overweight", "Obese"]


def test_dq_quarantines_bad_rows(spark):
    rows = [
        _row(),
        _row(age="250"),
        _row(bmi="abc"),
        _row(smoker=""),
        _row(region="central"),
        _row(charges="-10"),
        _row(charges="185000"),   # outlier -> sirf WARN, silver me jaayega
    ]
    valid, quarantine = apply_dq(clean_members(_df(spark, rows)), member_rules(REGIONS))
    errors = sorted(e for r in quarantine.collect() for e in r._dq_errors)
    assert errors == ["age_valid", "bmi_valid", "charges_positive", "region_in_master", "smoker_not_null"]
    assert valid.count() == 2
    assert valid.filter("charges > 100000").first()._dq_warnings == ["charges_outlier"]


def test_null_is_treated_as_failure(spark):
    # NULL charges: `charges > 0` NULL deta hai -> rule ko FAIL maanna chahiye
    _, q = apply_dq(clean_members(_df(spark, [_row(charges="")])), member_rules(REGIONS))
    assert q.first()._dq_errors == ["charges_positive"]


def test_record_id_dedupe_and_policy_year(spark):
    rows = [_row(), _row(_source_file="f2.csv"),                  # exact duplicate
            _row(_landing_date=dt.date(2027, 1, 1))]              # same member, next policy year
    out = dedupe(clean_members(_df(spark, rows)))
    assert out.count() == 2
    assert sorted(r.policy_year for r in out.collect()) == [2026, 2027]
