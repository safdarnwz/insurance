"""
Data Quality (DQ) framework - rules as data.

KYUN rules ko list me rakha (har jagah if/filter ki jagah):
  * Ek jagah sab rules -> business/actuarial team ke saath review ho sakta hai.
  * Naya rule = ek line, code change nahi.
  * Har row par kaunsa rule fail hua wo record hota hai -> debugging easy.

Severity:
  * "error" -> row SILVER me nahi, QUARANTINE table me (reason ke saath).
  * "warn"  -> row silver me jaati hai, `_dq_warnings` me flag (review ke liye).

KYA NAHI KARNA: bad rows ko chupchaap `.filter()` se drop mat karo. Kal finance
poochega "1500 aaye the, 1490 kyun?" - quarantine table hi jawab hai.

(Gold layer par dbt tests doosri line of defence hain - dbt/models/marts/_marts.yml)
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True)
class DQRule:
    name: str
    condition: str          # SQL expression jo VALID row ke liye TRUE ho
    severity: str = "error"  # error | warn


def member_rules(valid_regions: list[str]) -> list[DQRule]:
    """valid_regions silver.region_master se aata hai -> referential integrity check.
    KYUN dynamic: master me naya region add hua to code change nahi chahiye."""
    regions_sql = ", ".join(f"'{r}'" for r in sorted(valid_regions)) or "''"
    return [
        DQRule("age_valid", "age BETWEEN 18 AND 100"),
        DQRule("sex_valid", "sex IN ('male', 'female')"),
        DQRule("bmi_valid", "bmi BETWEEN 10 AND 80"),
        DQRule("children_valid", "children BETWEEN 0 AND 15"),
        DQRule("smoker_not_null", "is_smoker IS NOT NULL"),
        DQRule("region_in_master", f"region IN ({regions_sql})"),
        DQRule("charges_positive", "charges > 0"),
        # Outlier: genuine ho sakta hai (bada operation) -> drop nahi, flag
        DQRule("charges_outlier", "charges <= 100000", severity="warn"),
    ]


def _failed_rules_col(rules: list[DQRule]) -> Column:
    """Har row ke liye failed rule names ka array.
    coalesce(cond, false): NULL comparison ka result NULL hota hai -> usko FAIL maano.
    (Common bug: `charges > 0` NULL par NULL deta hai, filter chupchaap drop kar deta hai.)"""
    checks = [F.when(~F.coalesce(F.expr(r.condition), F.lit(False)), F.lit(r.name)) for r in rules]
    return F.filter(F.array(*checks), lambda x: x.isNotNull())


def apply_dq(df: DataFrame, rules: list[DQRule]) -> tuple[DataFrame, DataFrame]:
    """Returns (valid_df, quarantine_df)."""
    errors = [r for r in rules if r.severity == "error"]
    warns = [r for r in rules if r.severity == "warn"]
    tagged = df.withColumn("_dq_errors", _failed_rules_col(errors)).withColumn(
        "_dq_warnings", _failed_rules_col(warns)
    )
    valid = tagged.filter(F.size("_dq_errors") == 0).drop("_dq_errors")
    quarantine = tagged.filter(F.size("_dq_errors") > 0)
    return valid, quarantine
