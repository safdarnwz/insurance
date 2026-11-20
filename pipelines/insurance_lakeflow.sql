-- =============================================================================
-- LAKEFLOW DECLARATIVE PIPELINE  -  Insurance Medallion (Databricks-native)
-- (pehle "Delta Live Tables / DLT" kehte the)
-- -----------------------------------------------------------------------------
-- Aap sirf "kaunsi table chahiye" likhte ho (SQL). Databricks khud:
--   * dependency order (graph) banata hai
--   * incremental processing + checkpoints sambhalta hai (Auto Loader / streaming)
--   * expectations (data quality) ke pass/fail metrics UI me dikhata hai
--   * SCD1 / SCD2 (APPLY CHANGES) bina MERGE code ke
--   * retries, lineage, Unity Catalog tables
--
-- Source: SAME landing Volume jo Python job bharta hai (inbox -> ingest task).
-- Target schema: workspace.insurance_lakeflow
--
--   landing/kaggle/member_charges  ──► member_charges_bronze ──► member_charges_silver ──► member_charges (SCD1, dedupe)
--                                                        └──► member_charges_quarantine            │
--   landing/crm/region_master      ──► region_master_bronze ──► region_master_silver ──► region_scd2 (SCD2)
--                                                                                                   │
--                                         gold MVs: dim_region, dim_member_profile, fact_member_charges,
--                                                   agg_region_risk, agg_smoker_loading, agg_charges_by_segment
--
-- KYUN dono (Python job + ye pipeline)? Company me aksar EK chuna jaata hai:
--   * Python framework + dbt -> portable (laptop/Docker/any cloud), custom logic, unit tests
--   * Lakeflow pipeline       -> Databricks-native, kam code, built-in DQ/SCD/monitoring
-- Ye project dono dikhata hai taaki aap compare kar sako.
-- =============================================================================


-- =============================================================== BRONZE
-- Raw, sab STRING, append-only. Auto Loader (STREAM read_files) sirf nayi files padhta hai.
-- landing_date folder path (year=/month=/day=) se nikalte hain = business date.

CREATE OR REFRESH STREAMING TABLE member_charges_bronze
COMMENT 'BRONZE: raw member charges from landing Volume (Auto Loader). All columns STRING.'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  *,
  _metadata.file_path              AS _source_file,
  _metadata.file_name              AS _source_file_name,
  _metadata.file_modification_time AS _source_file_ts,
  make_date(
    cast(regexp_extract(_metadata.file_path, 'year=([0-9]{4})', 1) AS INT),
    cast(regexp_extract(_metadata.file_path, 'month=([0-9]{2})', 1) AS INT),
    cast(regexp_extract(_metadata.file_path, 'day=([0-9]{2})', 1) AS INT)
  )                                AS _landing_date,
  current_timestamp()              AS _ingest_ts
FROM STREAM read_files(
  '/Volumes/workspace/insurance_landing/files/kaggle/member_charges/',
  format => 'csv',
  header => true,
  fileNamePattern => '*.csv',            -- _manifest_*.json ignore
  schema => 'age STRING, sex STRING, bmi STRING, children STRING, smoker STRING, region STRING, charges STRING'
);

CREATE OR REFRESH STREAMING TABLE region_master_bronze
COMMENT 'BRONZE: raw region master (reference data from CRM)'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  *,
  _metadata.file_name              AS _source_file_name,
  _metadata.file_modification_time AS _source_file_ts,
  make_date(
    cast(regexp_extract(_metadata.file_path, 'year=([0-9]{4})', 1) AS INT),
    cast(regexp_extract(_metadata.file_path, 'month=([0-9]{2})', 1) AS INT),
    cast(regexp_extract(_metadata.file_path, 'day=([0-9]{2})', 1) AS INT)
  )                                AS _landing_date,
  current_timestamp()              AS _ingest_ts
FROM STREAM read_files(
  '/Volumes/workspace/insurance_landing/files/crm/region_master/',
  format => 'csv',
  header => true,
  fileNamePattern => '*.csv',
  schema => 'region STRING, region_name STRING, country STRING, regional_office STRING, risk_zone STRING'
);


-- =============================================================== SILVER
-- Typed + standardized + EXPECTATIONS (DQ rules).
--   ON VIOLATION DROP ROW    -> row silver me nahi (count "Data quality" tab me)
--   bina ON VIOLATION        -> sirf metric/warn (row rehti hai)
--   ON VIOLATION FAIL UPDATE -> poora update fail (critical rules)

CREATE OR REFRESH STREAMING TABLE member_charges_silver (
  CONSTRAINT age_valid        EXPECT (age BETWEEN 18 AND 100)            ON VIOLATION DROP ROW,
  CONSTRAINT sex_valid        EXPECT (sex IN ('male', 'female'))         ON VIOLATION DROP ROW,
  CONSTRAINT bmi_valid        EXPECT (bmi BETWEEN 10 AND 80)             ON VIOLATION DROP ROW,
  CONSTRAINT children_valid   EXPECT (children BETWEEN 0 AND 15)         ON VIOLATION DROP ROW,
  CONSTRAINT smoker_not_null  EXPECT (is_smoker IS NOT NULL)             ON VIOLATION DROP ROW,
  CONSTRAINT region_valid     EXPECT (region IN ('northeast','northwest','southeast','southwest')) ON VIOLATION DROP ROW,
  CONSTRAINT charges_positive EXPECT (charges > 0)                       ON VIOLATION DROP ROW,
  CONSTRAINT charges_outlier  EXPECT (charges <= 100000)                 -- warn only
)
COMMENT 'SILVER: typed, standardized, validated member charges (expectations)'
TBLPROPERTIES ('quality' = 'silver')
AS SELECT
  -- record_id: dataset me member_id nahi -> business columns + policy_year ka hash
  -- (same file dobara aaye to APPLY CHANGES duplicate nahi banata)
  sha2(concat_ws('||',
       cast(year(_landing_date) AS STRING), cast(age_i AS STRING), sex_c, cast(bmi_d AS STRING),
       cast(children_i AS STRING), cast(is_smoker AS STRING), region_c, cast(charges_d AS STRING)), 256) AS record_id,
  year(_landing_date)    AS policy_year,
  _landing_date          AS landing_date,
  age_i                  AS age,
  CASE WHEN age_i < 25 THEN '18-24' WHEN age_i < 35 THEN '25-34' WHEN age_i < 45 THEN '35-44'
       WHEN age_i < 55 THEN '45-54' WHEN age_i < 65 THEN '55-64' WHEN age_i IS NOT NULL THEN '65+' END AS age_band,
  sex_c                  AS sex,
  bmi_d                  AS bmi,
  CASE WHEN bmi_d < 18.5 THEN 'Underweight' WHEN bmi_d < 25 THEN 'Normal'
       WHEN bmi_d < 30 THEN 'Overweight' WHEN bmi_d IS NOT NULL THEN 'Obese' END AS bmi_category,
  children_i             AS children,
  is_smoker,
  region_c               AS region,
  charges_d              AS charges,
  _source_file_name,
  _source_file_ts,
  _ingest_ts
FROM (
  SELECT
    try_cast(trim(age) AS INT)                AS age_i,
    lower(trim(sex))                          AS sex_c,
    try_cast(trim(bmi) AS DECIMAL(5,2))       AS bmi_d,
    try_cast(trim(children) AS INT)           AS children_i,
    CASE lower(trim(smoker)) WHEN 'yes' THEN true WHEN 'no' THEN false END AS is_smoker,
    lower(trim(region))                       AS region_c,
    try_cast(trim(charges) AS DECIMAL(12,2))  AS charges_d,
    _landing_date, _source_file_name, _source_file_ts, _ingest_ts
  FROM STREAM(member_charges_bronze)
);

-- Quarantine: silver ki ulti condition. DHYAN: har condition coalesce(..., false) me -
-- warna NULL (jaise smoker khali) par NOT(... AND NULL) = NULL -> row na silver me,
-- na quarantine me (silent data loss). Ye bug pehle real run me pakda gaya tha.
CREATE OR REFRESH STREAMING TABLE member_charges_quarantine
COMMENT 'Rows rejected by silver expectations - source team ke saath fix ke liye'
TBLPROPERTIES ('quality' = 'quarantine')
AS SELECT * FROM STREAM(member_charges_bronze)
WHERE NOT (
      coalesce(try_cast(trim(age) AS INT) BETWEEN 18 AND 100, false)
  AND coalesce(lower(trim(sex)) IN ('male', 'female'), false)
  AND coalesce(try_cast(trim(bmi) AS DECIMAL(5,2)) BETWEEN 10 AND 80, false)
  AND coalesce(try_cast(trim(children) AS INT) BETWEEN 0 AND 15, false)
  AND coalesce(lower(trim(smoker)) IN ('yes', 'no'), false)
  AND coalesce(lower(trim(region)) IN ('northeast','northwest','southeast','southwest'), false)
  AND coalesce(try_cast(trim(charges) AS DECIMAL(12,2)) > 0, false)
);

-- Dedupe + upsert: APPLY CHANGES (SCD TYPE 1) on record_id.
-- KYUN: streaming me exact-duplicate / re-sent files ko MERGE code ke bina handle karna.
-- SEQUENCE BY: latest file jeetegi (out-of-order files bhi sahi).
CREATE OR REFRESH STREAMING TABLE member_charges
COMMENT 'SILVER (deduplicated): 1 row per member x policy year. SCD1 via APPLY CHANGES.';

APPLY CHANGES INTO member_charges
FROM STREAM(member_charges_silver)
KEYS (record_id)
SEQUENCE BY _source_file_ts
STORED AS SCD TYPE 1;

CREATE OR REFRESH STREAMING TABLE region_master_silver (
  CONSTRAINT region_not_null EXPECT (region IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT risk_zone_valid EXPECT (risk_zone IN ('Low', 'Medium', 'High'))
)
COMMENT 'SILVER: cleaned region master'
AS SELECT
  lower(trim(region))      AS region,
  trim(region_name)        AS region_name,
  trim(country)            AS country,
  trim(regional_office)    AS regional_office,
  trim(risk_zone)          AS risk_zone,
  _landing_date            AS effective_date,
  _source_file_ts
FROM STREAM(region_master_bronze);

-- SCD TYPE 2 - Databricks built-in. Value badli -> purani row band (__END_AT), nayi row
-- (__START_AT). TRACK HISTORY ON sirf business attributes: same values wali nayi file
-- se nayi version NAHI banti. SEQUENCE BY business date (landing date) -> point-in-time sahi.
CREATE OR REFRESH STREAMING TABLE region_scd2
COMMENT 'Region history (SCD Type 2 via APPLY CHANGES). __START_AT / __END_AT = validity.';

APPLY CHANGES INTO region_scd2
FROM STREAM(region_master_silver)
KEYS (region)
SEQUENCE BY effective_date
COLUMNS * EXCEPT (_source_file_ts)
STORED AS SCD TYPE 2
TRACK HISTORY ON region_name, country, regional_office, risk_zone;


-- =============================================================== GOLD
-- Materialized views: Databricks inhe (jahan possible) incrementally refresh karta hai.

CREATE OR REFRESH MATERIALIZED VIEW dim_region (
  CONSTRAINT region_sk_not_null EXPECT (region_sk IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT 'GOLD: region dimension, SCD2. Pehli version 1900-01-01 se valid (purane facts orphan na hon).'
AS SELECT
  md5(concat_ws('|', region, cast(__START_AT AS STRING)))                    AS region_sk,
  region, region_name, country, regional_office, risk_zone,
  CASE WHEN row_number() OVER (PARTITION BY region ORDER BY __START_AT) = 1
       THEN DATE'1900-01-01' ELSE __START_AT END                             AS effective_from,
  coalesce(__END_AT, DATE'9999-12-31')                                       AS effective_to,
  __END_AT IS NULL                                                           AS is_current
FROM region_scd2;

CREATE OR REFRESH MATERIALIZED VIEW dim_member_profile
COMMENT 'GOLD: junk dimension of member attributes + underwriting risk segment'
AS SELECT
  md5(concat_ws('|', age_band, sex, bmi_category, cast(is_smoker AS STRING), cast(children AS STRING))) AS profile_key,
  age_band, sex, bmi_category, is_smoker,
  CASE WHEN is_smoker THEN 'Smoker' ELSE 'Non-Smoker' END AS smoker_status,
  children,
  CASE WHEN is_smoker AND bmi_category = 'Obese' THEN 'Very High'
       WHEN is_smoker THEN 'High'
       WHEN bmi_category = 'Obese' THEN 'Medium'
       ELSE 'Low' END AS risk_segment
FROM (SELECT DISTINCT age_band, sex, bmi_category, is_smoker, children FROM member_charges);

-- Point-in-time join: member ka landing_date jis region version me aata hai wahi region_sk
CREATE OR REFRESH MATERIALIZED VIEW fact_member_charges (
  CONSTRAINT region_sk_found EXPECT (region_sk IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT charges_positive EXPECT (charges > 0) ON VIOLATION FAIL UPDATE
)
COMMENT 'GOLD: fact, 1 row per member per policy year'
AS SELECT
  m.record_id,
  m.policy_year,
  m.landing_date,
  r.region_sk,
  md5(concat_ws('|', m.age_band, m.sex, m.bmi_category, cast(m.is_smoker AS STRING), cast(m.children AS STRING))) AS profile_key,
  m.age, m.bmi, m.children,
  cast(m.is_smoker AS INT) AS smoker_flag,
  m.charges
FROM member_charges m
LEFT JOIN dim_region r
  ON  m.region = r.region
  AND m.landing_date >= r.effective_from
  AND m.landing_date <  r.effective_to;

CREATE OR REFRESH MATERIALIZED VIEW agg_region_risk
COMMENT 'GOLD KPI: region scorecard (current office / risk zone)'
AS SELECT
  r.region,
  c.regional_office                                                  AS current_office,
  c.risk_zone                                                        AS current_risk_zone,
  count(*)                                                           AS member_count,
  sum(f.charges)                                                     AS total_charges,
  round(avg(f.charges), 2)                                           AS avg_charges,
  round(avg(f.smoker_flag) * 100, 2)                                 AS smoker_pct,
  round(avg(CASE WHEN p.bmi_category = 'Obese' THEN 1 ELSE 0 END) * 100, 2) AS obese_pct
FROM fact_member_charges f
JOIN dim_region r         ON f.region_sk = r.region_sk
JOIN dim_member_profile p ON f.profile_key = p.profile_key
LEFT JOIN dim_region c    ON c.region = r.region AND c.is_current
GROUP BY r.region, c.regional_office, c.risk_zone;

CREATE OR REFRESH MATERIALIZED VIEW agg_smoker_loading
COMMENT 'GOLD KPI: smoker loading factor by age band (pricing input)'
AS SELECT
  p.age_band,
  round(avg(CASE WHEN f.smoker_flag = 0 THEN f.charges END), 2) AS avg_charges_non_smoker,
  round(avg(CASE WHEN f.smoker_flag = 1 THEN f.charges END), 2) AS avg_charges_smoker,
  count(*)                                                      AS member_count,
  round(avg(CASE WHEN f.smoker_flag = 1 THEN f.charges END)
        / nullif(avg(CASE WHEN f.smoker_flag = 0 THEN f.charges END), 0), 2) AS smoker_loading_factor
FROM fact_member_charges f
JOIN dim_member_profile p ON f.profile_key = p.profile_key
GROUP BY p.age_band;

CREATE OR REFRESH MATERIALIZED VIEW agg_charges_by_segment
COMMENT 'GOLD KPI: charges by year x region x smoker x BMI category'
AS SELECT
  f.policy_year, r.region, p.smoker_status, p.bmi_category,
  count(*)                               AS member_count,
  sum(f.charges)                         AS total_charges,
  round(avg(f.charges), 2)               AS avg_charges,
  round(percentile_approx(f.charges, 0.5), 2) AS median_charges,
  round(percentile_approx(f.charges, 0.9), 2) AS p90_charges
FROM fact_member_charges f
JOIN dim_member_profile p ON f.profile_key = p.profile_key
JOIN dim_region r         ON f.region_sk = r.region_sk
GROUP BY ALL;
