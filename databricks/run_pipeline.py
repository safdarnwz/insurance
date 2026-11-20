"""
Databricks job entry point (spark_python_task, serverless).

KYUN wheel ki jagah ye chhota script: Databricks Free Edition sirf serverless
compute deta hai, aur serverless par wheel install environment-version / caching
ki wajah se fiddly hai (purana code chal jaata hai). Bundle `src/` folder ko
workspace me sync karta hai; ye script use sys.path me daal ke wahi CLI chalata
hai jo laptop/Airflow par chalta hai -> ek hi code, har jagah.

Job parameters = CLI args, e.g. ["--env", "databricks", "run", "--layer", "bronze"]
"""

import os
import sys

try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:  # kuch Databricks contexts me __file__ nahi hota
    _here = os.path.dirname(os.path.abspath(sys.argv[0]))

sys.path.insert(0, os.path.abspath(os.path.join(_here, "..", "src")))

from insurance_lakehouse.main import main  # noqa: E402

if __name__ == "__main__":
    main(sys.argv[1:])
