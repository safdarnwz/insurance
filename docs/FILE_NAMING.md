# File naming & folder standard

## Standard (is project me implemented: `ingestion/landing.py`)

```
<landing>/<source_system>/<entity>/year=<YYYY>/month=<MM>/day=<DD>/
    <source_system>_<entity>_<YYYYMMDDTHHMMSSZ>_<batch_id>_part-<NNNNN>.<ext>
    _manifest_<source_system>_<entity>_<YYYYMMDDTHHMMSSZ>_<batch_id>.json
```

Example:
```
data/landing/kaggle/member_charges/year=2026/month=01/day=01/
    kaggle_member_charges_20260101T021500Z_4d3f1b75_part-00001.csv
    _manifest_kaggle_member_charges_20260101T021500Z_4d3f1b75.json
```

## Har hissa kyun

| Part | Example | Kyun |
|---|---|---|
| `source_system` | `kaggle`, `crm` | Ek hi entity kai systems se aa sakti hai; ownership clear |
| `entity` | `member_charges` | Ek folder = ek schema (Spark/Auto Loader ek schema expect karta hai) |
| `year=/month=/day=` | `year=2026/month=01/day=01` | **Hive-style partition**: Spark khud `year`, `month`, `day` columns banata hai; ek mahine/din ka data sirf ek folder (partition pruning); storage lifecycle rules prefix par |
| Business date (folder) | `2026-01-01` | Data **kis din ka hai** (Airflow logical date). Rerun/backfill same folder me jaata hai → idempotent |
| Extract timestamp (file) | `20260101T021500Z` | File **kab bani**. ISO-8601 basic, **UTC** (`Z`) → sortable, timezone confusion nahi |
| `batch_id` | `4d3f1b75` | Ek run ki saari files saath pehchaan me; same din me 2 baar run ho to naam collide nahi |
| `part-NNNNN` | `part-00001` | Badi file ke tukde; **zero-padded** taaki `part-10` `part-2` se pehle sort na ho |
| `_manifest_*.json` | rows, bytes, sha256 per part | Reconciliation (bronze rows == manifest rows) aur integrity. `_` se start → Spark ise data file nahi maanta |

## Month-wise, day-wise ya hour-wise?

| Data volume per day | Folder level |
|---|---|
| Kuch MB – kuch GB (ye project) | `year=/month=/day=` |
| Bahut zyada / near real-time (streaming events) | `year=/month=/day=/hour=` |
| Mahine me ek file (monthly extract) | `year=/month=` |

Rule: ek folder me itna data ho ki **files 128 MB – 1 GB** ki banein, aur folders ki ginti lakhon me na jaaye.

**`ingest_date=2026-01-01` (ek folder) vs `year=/month=/day=` (teen level):** dono industry me chalte hain.
Single date column query me aasaan hai (`WHERE ingest_date BETWEEN ...`); teen-level folders storage
browsing aur prefix-based lifecycle/ACL ke liye aasaan. Ye project landing par teen-level use karta hai aur
bronze me usse ek `_landing_date` column bana deta hai — dono ka fayda.

## Kya NAHI karna

| Galat | Problem |
|---|---|
| `Insurance Data FINAL v2 (1).csv` | Spaces/brackets scripts tod dete hain; "FINAL v2" ka koi matlab nahi |
| `data_01-02-2026.csv` | DD-MM ya MM-DD? Sort nahi hota |
| `insurance.csv` roz same naam se overwrite | History gayab, rerun/audit impossible, streaming nayi file nahi pehchaanta |
| Local time (IST) naam me | Daylight saving / multi-region me ghadi ulti-pulti |
| `/data/2026/Jan/` | Hive-style nahi → Spark partition columns nahi banenge; "Jan" sort nahi hota |
| Ek folder me alag schemas ki files | Auto Loader/Spark schema conflict |

## Tables naming (bonus)

* Schemas: `insurance_bronze`, `insurance_silver`, `insurance_gold` (domain + layer)
* Bronze: `<entity>_raw`; Silver: `<entity>`; Gold: `dim_*`, `fact_*`, `agg_*`
* Technical columns underscore se: `_source_file`, `_ingest_ts` (business columns se alag dikhein)
* Sab `snake_case`, lowercase, English
