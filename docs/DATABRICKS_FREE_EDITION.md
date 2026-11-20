# Databricks Free Edition — Industry-standard Lakehouse Architecture

Data **Kaggle se nahi**, seedha **Databricks ke data lake (Unity Catalog Volumes)** me aata hai
aur poora pipeline Databricks ke andar chalta hai. Free Edition ke saare main features use hote hain.

## 1. Architecture

```
                        DATABRICKS FREE EDITION  (Unity Catalog: catalog `workspace`)
 ┌─────────────────────────────────────────────────────────────────────────────────────────────┐
 │  DATA LAKE = UC VOLUMES  (schema insurance_landing)                                           │
 │   inbox/       <- source system file drop karta hai (UI upload / CLI / API)                    │
 │   files/       <- standard naming + manifest  (kaggle/member_charges/year=/month=/day=/...)    │
 │   checkpoints/ <- Auto Loader / streaming state                                                │
 │   artifacts/   <- code wheel (versioned)                                                       │
 │                                                                                                │
 │  LAKEFLOW JOB  "insurance-medallion-pipeline"  (serverless, file-arrival trigger)              │
 │   setup ─► ingest ─► bronze ─► reconcile ─► silver ─► dq_gate ─► gold_dbt ─► quality_report     │
 │             │        (Auto     (manifest   (DQ +      (circuit  (dbt on SQL   (notebook)        │
 │             │         Loader)   = bronze)   quarantine) breaker) warehouse)                    │
 │             └──────► lakeflow_pipeline  (Lakeflow Declarative Pipeline, expectations)          │
 │                                                                                                │
 │  DELTA TABLES (UC managed)                                                                     │
 │   insurance_bronze   member_charges_raw, region_master_raw                                     │
 │   insurance_silver   member_charges, member_charges_quarantine, region_master                  │
 │   insurance_snapshots snap_region_master (SCD2)                                                │
 │   insurance_gold     dim_region, dim_member_profile, fact_member_charges, agg_*                │
 │   insurance_lakeflow bronze/silver/quarantine + materialized view (DLT version)                │
 │   insurance_audit    pipeline_runs, silver_batch_stats                                         │
 │                                                                                                │
 │  CONSUME:  AI/BI Dashboard "Insurance Lakehouse - Overview"  |  SQL Editor  |  Notebook        │
 └─────────────────────────────────────────────────────────────────────────────────────────────┘
        ▲ deploy (Asset Bundle)                      ▲ trigger (optional)
   VS Code / Docker `dev` container              Airflow (Docker) - enterprise orchestrator
```

## 2. Free Edition ka har feature — project me kahan

| Databricks UI (left menu) | Project me | Kahan dekhein |
|---|---|---|
| **Catalog** (Unity Catalog) | 7 schemas, 4 volumes, managed Delta tables, schema **tags** + **comments**, table/column descriptions (dbt `persist_docs`), automatic **lineage** | Catalog → `workspace` → `insurance_gold` → `fact_member_charges` → *Lineage* tab |
| **Data Ingestion** | Volume `inbox` me file upload (drop zone) + **Auto Loader** (`cloudFiles`) | Catalog → `insurance_landing` → `inbox` → *Upload to this volume* |
| **Jobs & Pipelines → Jobs** | `insurance-medallion-pipeline` (9 tasks), `insurance-maintenance` (weekly OPTIMIZE/VACUUM) | Jobs & Pipelines |
| **Jobs & Pipelines → Pipelines** | `insurance-lakeflow-pipeline` (Lakeflow Declarative / DLT, expectations, materialized view) | Pipeline graph + *Data quality* tab |
| **Job Runs** | Har run ka task graph, duration, logs, retries; file-arrival trigger | Jobs & Pipelines → *Job runs* |
| **Notebooks** | `notebooks/insurance_analysis.py` — job ka last task (report) + interactive analysis | Workspace → `.bundle/insurance_lakehouse/dev/files/notebooks` |
| **SQL Warehouses / SQL Editor** | dbt gold isi par chalta hai (Serverless Starter Warehouse); ad-hoc queries | SQL Editor: `select * from workspace.insurance_gold.agg_region_risk` |
| **Dashboards** (AI/BI) | "Insurance Lakehouse - Overview": KPIs, region, smoker loading, BMI x smoker, risk segment, yearly | Dashboards |

## 3. Kyun aise (industry standard decisions)

| Decision | Kyun | Kya NAHI karna |
|---|---|---|
| **UC Volumes = data lake** (inbox/files/checkpoints/artifacts) | Governed storage: permissions, audit, lineage UC se. DBFS root deprecated pattern hai | DBFS root / `/tmp` me data rakhna |
| **Drop zone (inbox) → landing → archive** | Source kisi bhi naam se file daale; pipeline standard naam + manifest deta hai; original `_archive` me (proof + no reprocess) | Pipeline seedha source ki file overwrite/delete kare |
| **Code = versioned wheel in Volume** (`python_wheel_task`, `dynamic_version`) | Immutable artifact, rollback, same code laptop/Docker/Databricks | Production logic notebooks me copy-paste |
| **Lakeflow Job** with task-per-step | Fail par sirf wahi task repair; har step ka log/duration | Ek giant notebook |
| **File-arrival trigger** (PAUSED default) | Event-driven: file aate hi pipeline | Har 5 min polling schedule |
| **Serverless compute** | Free Edition requirement + no cluster management, auto-scale | — |
| **dbt on SQL warehouse** for gold | SQL + tests + docs + snapshots; warehouse BI ke liye optimized | Gold logic Python me hardcode |
| **Lakeflow Declarative Pipeline** (parallel) | Databricks-native option: expectations + DQ monitoring UI | Dono approaches same tables par likhein (yahan alag schema) |
| **Auth inside job = runtime identity** | dbt ko token Databricks SDK runtime se milta hai | Job parameters / code me PAT |
| **Asset Bundle** (`databricks.yml`) | Jobs, pipeline, dashboard sab code me, git review | UI me click-ops |

## 4. Setup (aapke laptop / Docker se) — ek baar

1. **Token** (Google/Gmail SSO users bhi): profile icon → *Settings* → *Developer* → *Access tokens* → *Generate new token*.
2. **SQL warehouse id**: *SQL Warehouses* → *Serverless Starter Warehouse* → URL / *Connection details* (HTTP path ka last part).
3. `.env` (git me nahi jaata):
   ```
   PIPELINE_ENV=databricks
   DATABRICKS_HOST=https://<workspace>.cloud.databricks.com
   DATABRICKS_TOKEN=<token>
   DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/<warehouse-id>
   UC_CATALOG=workspace
   DATABRICKS_BUNDLE_ENGINE=direct
   ```
4. Docker stack: `docker compose up -d --build`, phir:
   ```bash
   docker compose exec dev insurance-pipeline setup                       # schemas, volumes, tags
   docker compose exec dev databricks bundle deploy --var warehouse_id=<warehouse-id>
   docker compose exec dev insurance-pipeline upload-inbox --sample       # (ya UI se Volume me upload)
   docker compose exec dev databricks bundle run insurance_medallion --var warehouse_id=<warehouse-id>
   ```
5. Agla saal / naya data: `insurance-pipeline upload-inbox --synthetic-batch 2` (ya apni CSV UI se `inbox/kaggle/member_charges/` me) → job run.
6. Automatic chahiye: Job → *Triggers* → file arrival **Unpause** (ya Airflow DAG unpause — dono nahi).

## 5. Free Edition limitations jo code me handle ki hain

| Limitation / issue | Handling |
|---|---|
| Sirf serverless | `environments` (serverless env v4), koi cluster config nahi |
| Max 5 concurrent tasks | Main chain sequential; sirf Lakeflow pipeline parallel |
| Workspace files ke transient `403 / I/O error` | Code wheel se (Volume) + har task par `max_retries: 2` |
| `dbt_task` workspace files se project copy karta hai (same error) | dbt project wheel me packaged, `python_wheel_task` se dbt |
| Bundle deploy: `openpgp: key expired` (Terraform download) | `DATABRICKS_BUNDLE_ENGINE=direct` |
| API se naya catalog nahi banta | `workspace` catalog + `insurance_*` schemas (company me catalog per env: `insurance_dev/prod`) |
| Auto Loader + user schema + partition folders | `year/month/day` schema me add (`CF_MISSING_PARTITION_COLUMN_ERROR` fix) |
| Spark Connect: streaming progress None | Bronze row count Delta table se |
| foreachBatch alag process | `batch_df.sparkSession`, stats Delta table me |
| ANSI mode ON | `try_cast`, `nullif` |
| Daily compute quota | `availableNow` (stream chal ke ruk jaata hai) |

## 6. Kya NAHI karna

* Token sirf `.env` me. Chat, code, notebook, screenshot me kabhi nahi. Leak ho to turant revoke.
* File-arrival trigger **aur** Airflow schedule dono ON mat karo – double runs.
* `insurance_snapshots.snap_region_master` drop mat karo – SCD2 history wahi hai.
* Checkpoints volume delete karke "fix" mat karo – saara data dobara aa jaayega.
