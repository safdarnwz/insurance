# Insurance Lakehouse — ELT Medallion Pipeline

**Health insurance** domain par end-to-end data engineering project, company-standard tareeke se:

**Kaggle → Landing → Bronze → Silver (PySpark + Delta Lake) → Gold (dbt) — orchestrated by Airflow, sab Docker par.**

* Laptop par **sirf Docker Desktop + VS Code** chahiye. Azure/AWS account nahi.
* Wahi code **Databricks Free Edition** par bhi chalta hai (`PIPELINE_ENV=databricks`).
* Dataset: [Kaggle – Medical Cost Personal Datasets](https://www.kaggle.com/datasets/mirichoi0218/insurance) (`sample_data/insurance.csv`, 1338 rows: age, sex, bmi, children, smoker, region, charges)

Code me har jagah Hinglish comments hain: **kyun kiya, kya nahi karna, aisa karne se kya hota hai.**

---

## 1. Architecture (ELT)

```
            ┌──────────────────────────── Apache Airflow (Docker) ─────────────────────────────┐
            │  DAG insurance_elt (daily 02:15 UTC)                                              │
            │                                                                                   │
 Kaggle ──► land_to_landing ─► setup ─► bronze ─► reconcile ─► silver ─► dq_gate ─► dbt build ─► dbt docs
            │     (E)                    (L)        (check)      (T1)     (check)     (T2)       │
            └───────────────────────────────────────────────────────────────────────────────────┘

LANDING   data/landing/kaggle/member_charges/year=2026/month=01/day=01/
            kaggle_member_charges_20260101T021500Z_4d3f1b75_part-00001.csv  + _manifest_*.json
            (badi file -> 128 MB parts me auto-split)

BRONZE    Delta (Spark structured streaming, availableNow + checkpoint)  -> raw, sab STRING, append-only
SILVER    Delta (Spark foreachBatch MERGE) -> typed, DQ rules, quarantine table, dedupe
GOLD      dbt -> DuckDB (local) / Databricks SQL (cloud)
            snapshot  snap_region_master       (SCD Type 2)
            dim_region, dim_member_profile, fact_member_charges (incremental, point-in-time)
            agg_charges_by_segment, agg_smoker_loading, agg_region_risk
            + 28 dbt tests (unique, not_null, relationships, accepted_values, SCD2 integrity)
AUDIT     pipeline_runs, silver_batch_stats
```

| Sawaal | Jawab | Detail |
|---|---|---|
| ELT ya ETL? | **ELT** – raw pehle load (bronze), transform baad me platform ke andar (Spark + dbt) | [docs/ELT_vs_ETL.md](docs/ELT_vs_ETL.md) |
| 1 TB file? | Landing par split + chunked streaming + partition tuning | [docs/LARGE_FILES.md](docs/LARGE_FILES.md) |
| File naming? | `<system>_<entity>_<UTC ts>_<batch>_part-NNNNN.csv` in `year=/month=/day=/` + manifest | [docs/FILE_NAMING.md](docs/FILE_NAMING.md) |
| Databricks? | Free Edition: serverless job + UC Volumes + dbt-databricks | [docs/DATABRICKS_FREE_EDITION.md](docs/DATABRICKS_FREE_EDITION.md) |

### Tools — kaun kya karta hai

| Tool | Role | Kyun |
|---|---|---|
| **Airflow** | Orchestration: order, schedule, retry, history | Industry standard; har step alag task -> fail par sirf wahi retry |
| **PySpark + Delta Lake** | Bronze + Silver (heavy row-level kaam) | TB scale, ACID, MERGE, time travel, streaming checkpoints |
| **dbt** | Gold (business modelling) | SQL, ref() lineage, tests, docs, snapshots (SCD2) |
| **DuckDB** | Local data warehouse (gold) | Free, fast, single file; cloud me Databricks SQL warehouse |
| **Docker Compose** | Poora stack ek command me | "Mere laptop par chalta hai" problem khatam |
| **Databricks Free Edition** | Cloud Spark + Unity Catalog (optional) | Free, real lakehouse experience |

---

## 2. Quick start — Docker (recommended)

**Requirements:** Docker Desktop (6 GB+ RAM allocate karo), VS Code, Git.

```bash
git clone https://github.com/safdarnwz/insurance.git
cd insurance
cp .env.example .env            # local ke liye kuch badalna nahi (Linux: AIRFLOW_UID=$(id -u))
docker compose up -d --build    # pehli baar ~5-10 min (image build)
```

**Airflow UI:** http://localhost:8080  → login `airflow` / `airflow`

1. `insurance_elt` DAG ko **unpause** karo (toggle).
2. **Trigger DAG w/ config** → `{"source": "sample", "ingest_date": "2026-01-01"}` → saare tasks green.
3. Agla saal (ganda data + region office change): `{"source": "synthetic", "ingest_date": "2027-01-01"}`.
4. Results:
   ```bash
   docker compose exec dev insurance-pipeline show
   ```

VS Code me ye sab **Terminal → Run Task** me bhi hai (`airflow: trigger initial load`, `pipeline: show results`, …).
Code edit karo → container me turant reflect (repo mount hai), rebuild nahi.

**VS Code ko container ke andar kholna** (Java/Spark/dbt sab ready, Windows par bhi): *Dev Containers: Reopen in Container*.

**dbt docs + lineage graph:** Task `dbt: docs` → http://localhost:8081

**Band karna:** `docker compose down` (data `./data` me rehta hai). Sab saaf: `docker compose down -v` + `data/` delete.

### Bina Airflow ke (sirf CLI)
```bash
docker compose exec dev bash
insurance-pipeline setup
insurance-pipeline ingest --ingest-date 2026-01-01
insurance-pipeline run --layer bronze
insurance-pipeline reconcile
insurance-pipeline run --layer silver
insurance-pipeline dq-gate
insurance-pipeline run --layer gold          # dbt build
insurance-pipeline show
```

### Large file demo
```bash
insurance-pipeline generate-large --rows 2000000                       # ~80 MB
insurance-pipeline ingest --source file --path data/tmp/large_member_charges.csv \
    --split-mb 16 --chunk-mb 16 --ingest-date 2028-01-01               # -> 5 parts + manifest
insurance-pipeline run --layer bronze && insurance-pipeline reconcile
```

### Bina Docker ke (local venv, Linux/Mac/WSL — Python 3.11 + Java 17)
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt && pip install -e .
pytest -q -p no:logging
```
(Windows native par Spark ko `winutils.exe` chahiye – isliye Docker/Dev Container use karo.)

---

## 3. Databricks Free Edition par (optional)

Gmail/Google SSO se login wale accounts ke liye bhi same steps (password ki zaroorat nahi):

1. Databricks → profile icon → **Settings → Developer → Access tokens → Generate new token**.
2. **SQL Warehouses → Serverless Starter Warehouse → Connection details → HTTP path** copy.
3. `.env` me: `PIPELINE_ENV=databricks`, `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_HTTP_PATH`.
4. `docker compose up -d` → `docker compose exec dev databricks bundle deploy` → Airflow me DAG trigger.

Poori detail + Free Edition limitations: [docs/DATABRICKS_FREE_EDITION.md](docs/DATABRICKS_FREE_EDITION.md).

---

## 4. Verified results (is repo par chala ke dekha)

| Step | Result |
|---|---|
| Docker stack | postgres, airflow-apiserver, scheduler, dag-processor — sab healthy |
| DAG run (2026, sample) | 9/9 tasks success |
| Bronze | 1338 + 4 rows; reconcile OK (manifest = bronze) |
| Silver | **1337** (Kaggle file me 1 exact duplicate) |
| DAG run (2027, synthetic) | 204 rows → **195** silver, **6 quarantined** (age 250, bmi "abc", missing smoker, region "central", negative charges, broken CSV line), 3 duplicates; DQ gate 2.94% < 5% |
| dbt build | 37/37 pass (9 models/snapshot + 28 tests) |
| SCD2 | southeast: Atlanta (1900→2027-01-01) → Miami (2027-01-01→9999); 2026 facts Atlanta se, 2027 facts Miami se |
| Rerun same data | 0 new rows — idempotent |
| Large file | 500k rows → 3 parts; bronze 500,000 rows; reconcile OK |
| pytest | 10 passed |

Databricks target: `dbt parse --target databricks` pass; Databricks job/warehouse par actual run abhi test nahi hua (workspace access nahi tha).

Insight (`agg_smoker_loading`): 18-24 age me smoker ka average charge non-smoker se **~7x**, 55-64 me ~2.75x.

---

## 5. Folder structure

```
├── airflow/dags/                 insurance_elt.py (main DAG), insurance_maintenance.py (weekly OPTIMIZE/VACUUM)
├── dbt/                          dbt project (gold): models/staging, models/marts, snapshots, tests, macros
├── src/insurance_lakehouse/
│   ├── conf/config.yaml          paths, table names, large-file settings, DQ gate (koi secret nahi)
│   ├── ingestion/                landing.py (naming + split + manifest), synthetic_data.py
│   ├── bronze/ silver/           Spark layers
│   ├── gold/dbt_runner.py        dbt build wrapper
│   ├── quality/                  dq_checks (rules), reconcile (manifest vs bronze), gate (circuit breaker)
│   ├── utils/                    delta helpers (local path + Unity Catalog), audit, maintenance
│   └── main.py                   CLI: insurance-pipeline
├── databricks.yml + databricks/  Databricks Free Edition job (serverless)
├── notebooks/                    Databricks analysis notebook
├── tests/                        pytest: landing, transformations, e2e (Spark + dbt)
├── docs/                         ELT vs ETL, large files, file naming, Databricks setup
├── Dockerfile, docker-compose.yml, .devcontainer/, .vscode/
└── sample_data/                  insurance.csv (Kaggle) + region_master.csv
```

---

## 6. Company-standard practices (code me har jagah explained)

| Kiya | Kyun | Kya NAHI karna |
|---|---|---|
| Config-driven (`config.yaml`), env se switch | Same code local/Databricks | Paths hardcode, `if env == prod` har jagah |
| Secrets sirf `.env` (gitignored) | Leak-proof | Token code/git/chat me |
| Standard file naming + manifest | Sortable, idempotent, audit-able | `final_v2 (1).csv`, same naam overwrite |
| Bronze all-STRING + `_corrupt_record` | Original data kabhi lose na ho | Bronze me cast/filter |
| Streaming `availableNow` + checkpoint | Sirf nayi files, exactly-once | Har run full overwrite |
| Reconciliation (manifest vs bronze) | Silent data loss pakdo | "Job green hai to data sahi hai" maan lena |
| DQ rules → quarantine + DQ gate | Har reject ka reason; kharab batch gold tak nahi | Bad rows chupchaap drop |
| `try_cast`, `DECIMAL` for money | ANSI-safe, exact paisa | `DOUBLE` for currency |
| MERGE / incremental dbt | Idempotent reruns | Plain append (rerun = duplicates) |
| dbt snapshot SCD2 by business date | Point-in-time sahi history | Snapshot table drop / full-refresh |
| dbt tests + pytest + CI | Galat numbers prod se pehle pakdo | Bina test deploy |
| Airflow: ek task = ek step, `max_active_runs=1`, catchup off | Retry/repair granular, no overlap | Ek giant script |
| Docker image me sab pinned (Delta jars bhi) | Reproducible, offline start | Container start par pip install |

### Ek zaroori dbt lesson
`dbt snapshot` sirf wahi history pakadta hai jo **har run me dekhta hai**. Agar region master do baar badla aur beech me gold nahi chala, to beech wala version kabhi record nahi hoga. Isliye DAG har load ke baad gold chalata hai (aur e2e test bhi yahi check karta hai).

---

## 7. Dataset limitations (honest note)

* Kaggle data me **member_id/policy_id nahi** → `record_id` = business columns + policy_year ka SHA-256 hash. Do bilkul same rows = ek record. Real project me source se proper ID maango.
* Data clean hai, isliye DQ/incremental/SCD2 dikhane ke liye `--source synthetic` same statistical pattern ka next-year data + ganda data + region change bhejta hai.
* Health data sensitive (PHI) hota hai – real project me masking/row-level security zaroori.
