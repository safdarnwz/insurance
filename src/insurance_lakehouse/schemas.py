"""
Source schemas (data contracts).

1) member_charges  (Kaggle: mirichoi0218/insurance -> insurance.csv)
   age, sex, bmi, children, smoker, region, charges
   Har row = ek insured member ka ek policy year ka medical charge.

2) region_master   (company ka internal reference data - sample_data/region_master.csv)
   region, region_name, country, regional_office, risk_zone

KYUN explicit schema (inferSchema=True ki jagah):
  * inferSchema poori file 2 baar padhta hai -> 1 TB par ghanton ka extra kaam.
  * Kal source ne age me "45 yrs" bheja to inferred type badal jaayega aur
    downstream sab tootega. Explicit schema = data contract.

KYUN Bronze me sab STRING:
  Bronze ka kaam "jaisa aaya waisa save karo" hai. Bronze me cast kiya aur value
  "abc" aayi to wo NULL ban jaayegi aur original value hamesha ke liye gayi.
  Casting + validation SILVER me hota hai (fail rows -> quarantine).

Column lists plain Python hain (pyspark import nahi) - taaki landing/Airflow
code bina Spark ke bhi ye module use kar sake.
"""

CORRUPT_RECORD_COL = "_corrupt_record"

MEMBER_COLUMNS = ["age", "sex", "bmi", "children", "smoker", "region", "charges"]
REGION_COLUMNS = ["region", "region_name", "country", "regional_office", "risk_zone"]
SOURCE_COLUMNS = {"member_charges": MEMBER_COLUMNS, "region_master": REGION_COLUMNS}


def raw_schema(source: str):
    """Bronze read schema: sab STRING + _corrupt_record.
    _corrupt_record: PERMISSIVE mode me jo line parse nahi hui wo yahan poori raw
    text me aati hai -> data loss nahi, debug kar sakte ho."""
    from pyspark.sql.types import StringType, StructField, StructType

    return StructType(
        [StructField(c, StringType(), True) for c in SOURCE_COLUMNS[source]]
        + [StructField(CORRUPT_RECORD_COL, StringType(), True)]
    )
