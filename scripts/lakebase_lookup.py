"""
APP-SIDE EXAMPLE: Lakebase Postgres se member risk lookup (serving layer ka consumer).

Ye wo code hai jo ek insurance app / API backend (FastAPI, Django, Java service)
likhega. Lakehouse ke andar ka kuch nahi - sirf standard Postgres driver.

Auth: password NAHI. Databricks se short-lived OAuth token (1 ghanta) liya jaata hai
-> wahi Postgres password ki jagah. Token leak bhi ho to jaldi expire.
KYA NAHI KARNA: app me DATABRICKS_TOKEN ya Postgres password hardcode.
Production me app service principal ki identity use kare.

Run (laptop se, .env me DATABRICKS_HOST/TOKEN):
  pip install "psycopg[binary]" databricks-sdk
  python scripts/lakebase_lookup.py                 # top high-risk members + region scorecard
  python scripts/lakebase_lookup.py <record_id>     # ek member ka lookup
"""

import os
import re
import sys
import time
from pathlib import Path

for line in (Path(__file__).resolve().parents[1] / ".env").read_text().splitlines():
    m = re.match(r"^([A-Z_]+)=(.*)$", line.strip())
    if m:
        os.environ.setdefault(m[1], m[2])

import psycopg  # noqa: E402
from databricks.sdk import WorkspaceClient  # noqa: E402

PROJECT, BRANCH, ENDPOINT, DB = "insurance-serving", "production", "primary", "databricks_postgres"

w = WorkspaceClient()
ep_name = f"projects/{PROJECT}/branches/{BRANCH}/endpoints/{ENDPOINT}"
endpoint = w.api_client.do("GET", f"/api/2.0/postgres/{ep_name}")
host = endpoint["status"]["hosts"]["host"]
cred = w.api_client.do("POST", "/api/2.0/postgres/credentials", body={"endpoint": ep_name})
user = w.current_user.me().user_name

t0 = time.perf_counter()
with psycopg.connect(host=host, port=5432, dbname=DB, user=user, password=cred["token"], sslmode="require") as conn:
    print(f"connected in {(time.perf_counter() - t0) * 1000:.0f} ms (scale-to-zero ho to pehli baar thoda slow)")
    cur = conn.cursor()
    # Synced tables Postgres me UC schema ke naam wale schema me banti hain
    cur.execute("select table_schema, table_name from information_schema.tables "
                "where table_schema = 'insurance_serving'")
    print("tables:", cur.fetchall())

    if len(sys.argv) > 1:
        t0 = time.perf_counter()
        cur.execute("select * from insurance_serving.serve_member_risk where record_id = %s", (sys.argv[1],))
        print(cur.fetchone(), f"\nlookup: {(time.perf_counter() - t0) * 1000:.1f} ms")
    else:
        cur.execute("""select record_id, region, regional_office, risk_segment, charges, cost_index
                       from insurance_serving.serve_member_risk order by cost_index desc limit 5""")
        for row in cur.fetchall():
            print(row)
        cur.execute("select region, current_office, member_count, avg_charges from insurance_serving.agg_region_risk")
        for row in cur.fetchall():
            print(row)
