# Databricks Free Edition par chalana

Free Edition = personal, free, **sirf serverless compute**, Unity Catalog ke saath. Ye project usi ke hisaab se bana hai.

## Architecture (PIPELINE_ENV=databricks)

```
 Laptop (Docker)                                   Databricks Free Edition
 ┌──────────────────────────────┐                  ┌──────────────────────────────────────────┐
 │ Airflow                      │  Files API       │ UC Volume: workspace.insurance_landing   │
 │  land_to_landing  ───────────┼─────────────────►│   /files/kaggle/member_charges/year=...  │
 │                              │                  │                                          │
 │  bronze_silver_on_databricks ┼─ Jobs API ──────►│ Job "insurance-bronze-silver" (serverless)│
 │   (DatabricksRunNowOperator) │  (wait)          │   setup → bronze → reconcile → silver →  │
 │                              │                  │   dq_gate      (Spark, Auto Loader)      │
 │  gold_dbt_build  ────────────┼─ SQL ───────────►│ Serverless Starter SQL Warehouse         │
 │   (dbt-databricks)           │                  │   workspace.insurance_gold.* (dbt)       │
 └──────────────────────────────┘                  └──────────────────────────────────────────┘
```

**Kyun Kaggle download laptop par (Databricks par nahi):** serverless compute ka internet access restricted ho
sakta hai, aur ingestion (data movement) ko compute se alag rakhna standard hai.

## One-time setup (15 min)

1. **Token:** Databricks → right-top profile → *Settings* → *Developer* → *Access tokens* → *Generate new token*.
2. **SQL warehouse HTTP path:** *SQL Warehouses* → *Serverless Starter Warehouse* → *Connection details* → *HTTP path*.
3. `.env` me (git me nahi jaata):
   ```
   PIPELINE_ENV=databricks
   DATABRICKS_HOST=https://<aapka-workspace>.cloud.databricks.com
   DATABRICKS_TOKEN=<token>
   DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/<id>
   UC_CATALOG=workspace
   ```
   Host = browser URL ka sirf `https://...cloud.databricks.com` part (`?o=...` waghera nahi).
4. Stack restart (naya env load ho): `docker compose up -d`
5. Job deploy (Docker `dev` container me Databricks CLI already hai):
   ```
   docker compose exec dev databricks bundle validate
   docker compose exec dev databricks bundle deploy
   ```
6. Airflow UI → `insurance_elt` → Trigger. Ya terminal se:
   ```
   docker compose exec dev databricks bundle run insurance_bronze_silver   # sirf Spark part
   docker compose exec dev insurance-pipeline --env databricks run --layer gold   # dbt
   ```
7. Results: Databricks → *SQL Editor* → `select * from workspace.insurance_gold.agg_region_risk`.
   `notebooks/insurance_analysis.py` ko workspace me import karke bhi dekh sakte ho.

## Free Edition limitations jo code me handle ki hain

| Limitation | Code me kya kiya |
|---|---|
| Sirf serverless (cluster nahi bana sakte) | `databricks.yml` me `environments` (serverless), koi `new_cluster` nahi |
| Max 5 concurrent job tasks | Tasks sequential, `max_concurrent_runs: 1` |
| Zyada tar Spark configs allowed nahi | `spark_session._safe_set` – fail par warning, crash nahi |
| foreachBatch alag process me chalta hai | `batch_df.sparkSession` use, stats Delta table me (Python list me nahi) |
| ANSI SQL mode ON | `try_cast`, `nullif` division |
| DBFS limited | Files + checkpoints **UC Volumes** me |
| Wheel install serverless par fiddly | `spark_python_task` + bundle `src/` sync (`databricks/run_pipeline.py`) |
| Daily compute quota | Chhota data; `availableNow` trigger (stream chal ke ruk jaata hai, idle compute nahi) |

## Kya NAHI karna

* Token `.env` ke alawa kahin mat likho (code, notebook, chat, screenshot).
* Airflow schedule **aur** Databricks job schedule dono ON mat karo – job do baar chalega.
* Snapshot table (`insurance_snapshots.snap_region_master`) drop mat karo – SCD2 history wahi hai.
