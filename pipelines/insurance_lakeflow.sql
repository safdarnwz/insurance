-- =============================================================================
-- Lakeflow Declarative Pipeline (pehle "Delta Live Tables / DLT" kehte the)
-- -----------------------------------------------------------------------------
-- Databricks ka NATIVE declarative tareeka: aap sirf "kya table chahiye" likhte
-- ho (SQL), Databricks khud dependency order, incremental processing, retries,
-- checkpoints, lineage aur data-quality metrics sambhalta hai.
--
-- Is project me main pipeline Python job (src/) hai - laptop/Docker par bhi chale.
-- Ye pipeline SAME landing data par Databricks-native version dikhata hai
-- (schema: insurance_lakeflow). Company me aksar inme se EK approach chuni jaati hai:
--   * Python framework (job)   -> portable, custom logic, local testing
--   * Lakeflow pipeline (ye)   -> kam code, built-in DQ (expectations) + monitoring UI
--
-- UI: Jobs & Pipelines -> insurance-lakeflow-pipeline -> graph + "Data quality" tab
-- =============================================================================

-- BRONZE: Auto Loader (read_files STREAM) - sirf nayi files, sab STRING
CREATE OR REFRESH STREAMING TABLE member_charges_bronze
COMMENT 'Raw member charges from landing volume (Auto Loader). All columns STRING.'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  *,
  _metadata.file_name          AS _source_file_name,
  _metadata.file_modification_time AS _source_file_ts,
  current_timestamp()          AS _ingest_ts
FROM STREAM read_files(
  '/Volumes/workspace/insurance_landing/files/kaggle/member_charges/',
  format => 'csv',
  header => true,
  fileNamePattern => '*.csv',
  schema => 'age STRING, sex STRING, bmi STRING, children STRING, smoker STRING, region STRING, charges STRING'
);

-- SILVER: typed + EXPECTATIONS (DQ rules).
-- (Simple rakha hai: dedupe / MERGE nahi - wo Python job wale silver me hai.
--  Lakeflow me dedupe ke liye AUTO CDC / APPLY CHANGES INTO use hota hai.)
--   ON VIOLATION DROP ROW -> row silver me nahi aayegi (count UI me dikhta hai)
--   bina ON VIOLATION      -> sirf warn/metric (row rehti hai)
--   ON VIOLATION FAIL UPDATE -> poora update fail (critical rules ke liye)
CREATE OR REFRESH STREAMING TABLE member_charges_silver (
  CONSTRAINT age_valid        EXPECT (age BETWEEN 18 AND 100)            ON VIOLATION DROP ROW,
  CONSTRAINT sex_valid        EXPECT (sex IN ('male', 'female'))         ON VIOLATION DROP ROW,
  CONSTRAINT bmi_valid        EXPECT (bmi BETWEEN 10 AND 80)             ON VIOLATION DROP ROW,
  CONSTRAINT smoker_not_null  EXPECT (is_smoker IS NOT NULL)             ON VIOLATION DROP ROW,
  CONSTRAINT region_valid     EXPECT (region IN ('northeast','northwest','southeast','southwest')) ON VIOLATION DROP ROW,
  CONSTRAINT charges_positive EXPECT (charges > 0)                       ON VIOLATION DROP ROW,
  CONSTRAINT charges_outlier  EXPECT (charges <= 100000)
)
COMMENT 'Cleaned member charges with data-quality expectations'
TBLPROPERTIES ('quality' = 'silver')
AS SELECT
  try_cast(age AS INT)               AS age,
  lower(trim(sex))                   AS sex,
  try_cast(bmi AS DECIMAL(5,2))      AS bmi,
  try_cast(children AS INT)          AS children,
  CASE lower(trim(smoker)) WHEN 'yes' THEN true WHEN 'no' THEN false END AS is_smoker,
  lower(trim(region))                AS region,
  try_cast(charges AS DECIMAL(12,2)) AS charges,
  _source_file_name,
  _ingest_ts
FROM STREAM(member_charges_bronze);

-- Quarantine: jo rows silver me DROP hui unka record (DLT pattern: ulti condition)
-- DHYAN: har condition coalesce(..., false) me - warna NULL (jaise smoker khali)
-- par NOT(... AND NULL) = NULL hota hai aur row na silver me, na quarantine me
-- (silent data loss). Ye bug pehle run me pakda gaya tha: 1542 = 1536 + 5.
CREATE OR REFRESH STREAMING TABLE member_charges_quarantine
COMMENT 'Rows rejected by silver expectations - source team ke saath fix ke liye'
AS SELECT * FROM STREAM(member_charges_bronze)
WHERE NOT (
  coalesce(try_cast(age AS INT) BETWEEN 18 AND 100, false)
  AND coalesce(lower(trim(sex)) IN ('male', 'female'), false)
  AND coalesce(try_cast(bmi AS DECIMAL(5,2)) BETWEEN 10 AND 80, false)
  AND coalesce(lower(trim(smoker)) IN ('yes', 'no'), false)
  AND coalesce(lower(trim(region)) IN ('northeast','northwest','southeast','southwest'), false)
  AND coalesce(try_cast(charges AS DECIMAL(12,2)) > 0, false)
);

-- GOLD: Materialized view - Databricks khud incremental refresh karta hai
CREATE OR REFRESH MATERIALIZED VIEW charges_by_region_smoker
COMMENT 'Avg charges by region and smoker status'
AS SELECT
  region,
  CASE WHEN is_smoker THEN 'Smoker' ELSE 'Non-Smoker' END AS smoker_status,
  count(*)                  AS members,
  round(avg(charges), 2)    AS avg_charges,
  round(sum(charges), 2)    AS total_charges
FROM member_charges_silver
GROUP BY ALL;
