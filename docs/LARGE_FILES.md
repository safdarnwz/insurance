# 1 TB file aaye to kya karein? (Partitioning + Large file strategy)

Hamari Kaggle file 55 KB ki hai, lekin pipeline aise design hai ki 1 TB bhi isi code se chale.
Neeche har step ka **kya, kyun, aur kya nahi karna**.

---

## Step 0 — Source se baat karo (sabse sasta fix)

| Pehle maango | Kyun |
|---|---|
| **Ek 1 TB file nahi, 128 MB – 1 GB ke kai files** | Parallel upload/download, retry sirf ek part ka |
| **Parquet** (CSV nahi) | Columnar + compressed (CSV se ~5-10x chhota), schema andar hota hai, splittable |
| Compression: **zstd/snappy (Parquet ke andar)** ya bzip2 | Splittable – Spark ek file ko kai tasks me padh sakta hai |
| **Manifest file** (rows, size, checksum) | Load ke baad reconcile kar sako |

**KYA NAHI KARNA: 1 TB ki `.csv.gz` file.** gzip **splittable nahi** hai – Spark poori file **ek hi task** (ek CPU core) se padhega. 1000 cores ka cluster bhi 1 core jitna slow. Pehle decompress + split karo.

---

## Step 1 — Landing par split (ye project karta hai)

`insurance-pipeline ingest --source file --path big.csv` – file `large_files.split_threshold_mb` (256 MB) se badi ho to
`split_chunk_mb` (128 MB) ke parts me todi jaati hai (`src/insurance_lakehouse/ingestion/landing.py: split_csv`):

```
landing/kaggle/member_charges/year=2026/month=01/day=01/
  kaggle_member_charges_20260101T020000Z_a1b2c3d4_part-00001.csv   (128 MB, header ke saath)
  kaggle_member_charges_20260101T020000Z_a1b2c3d4_part-00002.csv
  ...
  kaggle_member_charges_20260101T020000Z_a1b2c3d4_part-08192.csv
  _manifest_kaggle_member_charges_20260101T020000Z_a1b2c3d4.json  (har part ki rows + sha256)
```

* Line-by-line stream → **memory constant** (1 TB file ke liye bhi kuch MB RAM).
* Har part me header → har part independently padha ja sakta hai.
* Demo: `insurance-pipeline generate-large --rows 2000000` phir `ingest --source file --path data/tmp/large_member_charges.csv --split-mb 16 --chunk-mb 16`.

---

## Step 2 — Bronze: chunks me load (backpressure)

`config.yaml → large_files.max_files_per_trigger: 100`

* 8192 parts / 100 = ~82 micro-batches. **Har batch commit hota hai** (checkpoint). Beech me fail hua to sirf wahi batch dobara – poora TB nahi.
* Databricks Auto Loader: `cloudFiles.maxFilesPerTrigger` / `cloudFiles.maxBytesPerTrigger`.

---

## Step 3 — Spark partitions (parallelism) — calculation

**Input partitions** (`spark.sql.files.maxPartitionBytes = 128 MB`):
```
1 TB / 128 MB = 8192 tasks
```
Cluster me 64 cores → 8192 / 64 = **128 "waves"**. Har task ~128 MB → executor memory me aaram se.

**Shuffle partitions** (join/groupBy/dedupe ke baad):
```
shuffle data / 128-200 MB  =  spark.sql.shuffle.partitions
1 TB shuffle → ~5000–8000
```
* Default 200 → har shuffle partition 5 GB → memory spill / OOM.
* **AQE** (Adaptive Query Execution, Spark 3+ / Databricks default ON) chhote partitions khud merge karta hai aur skew handle karta hai. Databricks serverless par ye sab automatic hai (isliye config me `shuffle_partitions: auto`).

---

## Step 4 — Storage partitioning (disk par folders)

| Table | Partition column | Kyun |
|---|---|---|
| Landing | `year=/month=/day=` (business date) | "January ka data" = sirf us folder ko padho, lifecycle rules (30 din baad cool tier) |
| Bronze | `_ingest_date` | Replay/debug "us din jo aaya" se hota hai |
| Silver/Gold | **Partition nahi** + Z-ORDER / Liquid Clustering | 1 TB se chhoti tables me partitioning se nuksaan zyada |

**Rules of thumb:**
* Har partition me **kam se kam ~1 GB** data ho. Isse kam = small files problem.
* Partition column **low cardinality** ho: date, year, region. **Kabhi nahi:** `member_id`, `policy_number`, timestamp (lakhon folders).
* Query ke `WHERE` me jo column sabse zyada aata hai wahi partition/cluster key.
* Databricks par naye tables ke liye **Liquid Clustering** (`CLUSTER BY`) partitioning se better – baad me key badal sakte ho.

---

## Step 5 — Output file size

* Target: **128 MB – 1 GB per file**.
* `optimizeWrite` (config me ON) write ke waqt files jodta hai; weekly `OPTIMIZE` (maintenance DAG) baaki compact karta hai.
* **Kya nahi karna:** `df.coalesce(1).write...` "ek file chahiye" ke liye – poora TB ek task se likha jaayega.

---

## Step 6 — Code me 1 TB par jo cheezein tootti hain (avoid karo)

| Mat karo | Kyun | Iski jagah |
|---|---|---|
| `df.collect()`, `df.toPandas()` | Poora data driver (ek machine) par | `write` / aggregate karke chhota result |
| `inferSchema=True` | Poori file extra baar padhta hai | Explicit schema (`schemas.py`) |
| `df.count()` baar-baar | Har baar poora scan | Ek baar, ya metrics Delta history se |
| Python UDF | Har row JVM↔Python | Built-in `F.*` functions |
| Bada table × bada table join bina soche | Huge shuffle | Chhote table ko `broadcast()`; skew par AQE/salting |
| Har run full overwrite | Har din 1 TB rewrite | Incremental (checkpoint + MERGE) – yahi project karta hai |

---

## Step 7 — Gold (dbt) at scale

Local DuckDB laptop ke liye hai (kuch GB tak theek). TB scale par **same dbt models** `--target databricks` par chalte hain
(Databricks SQL warehouse) – `fact_member_charges` already **incremental (MERGE)** hai, to roz sirf naye records process hote hain.
