# Databricks notebook source
# MAGIC %md
# MAGIC # Insurance Lakehouse - Gold layer analysis
# MAGIC Ye notebook **sirf exploration / analysis** ke liye hai. Pipeline ka asli code
# MAGIC `src/insurance_lakehouse` (Spark) aur `dbt/` (gold) me hai; Airflow + Databricks Job se chalta hai.
# MAGIC
# MAGIC **KYA NAHI KARNA:** production pipeline logic notebooks me copy-paste mat karo -
# MAGIC test nahi hota, version mess hota hai. Notebooks = explore, package = production.
# MAGIC
# MAGIC VS Code me: Databricks extension se "Run File as Workflow" ya Databricks Connect se cell-by-cell.

# COMMAND ----------

catalog = "workspace"  # Free Edition default catalog
spark.sql(f"USE CATALOG {catalog}")  # noqa: F821

# COMMAND ----------

# MAGIC %md ## 1. Pipeline health - aaj ka load chala?

# COMMAND ----------

display(spark.sql("""
  SELECT layer, status, start_ts, duration_sec, rows_in, rows_out, rows_quarantined, error_message
  FROM insurance_audit.pipeline_runs ORDER BY start_ts DESC LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md ## 2. Data quality - kya reject hua aur kyun?

# COMMAND ----------

display(spark.sql("""
  SELECT explode(_dq_errors) AS rule, count(*) AS rows
  FROM insurance_silver.member_charges_quarantine GROUP BY 1 ORDER BY 2 DESC
"""))

# COMMAND ----------

# MAGIC %md ## 3. Smoker loading factor (pricing input)

# COMMAND ----------

display(spark.table("insurance_gold.agg_smoker_loading").orderBy("age_band"))

# COMMAND ----------

# MAGIC %md ## 4. Risk segment wise average charges (star schema join)

# COMMAND ----------

display(spark.sql("""
  SELECT p.risk_segment, r.region, r.regional_office,
         count(*) AS members, round(avg(f.charges), 2) AS avg_charges
  FROM insurance_gold.fact_member_charges f
  JOIN insurance_gold.dim_member_profile p ON f.profile_key = p.profile_key
  JOIN insurance_gold.dim_region r         ON f.region_sk   = r.region_sk
  GROUP BY ALL ORDER BY avg_charges DESC
"""))

# COMMAND ----------

# MAGIC %md ## 5. Delta time travel - kal table kaisi thi?

# COMMAND ----------

display(spark.sql("DESCRIBE HISTORY insurance_silver.member_charges"))
# display(spark.sql("SELECT count(*) FROM insurance_silver.member_charges VERSION AS OF 0"))

# COMMAND ----------

# MAGIC %md ## 6. Lakeflow Declarative Pipeline vs Python+dbt - same answer?
# MAGIC `insurance_lakeflow` schema = Databricks-native pipeline (expectations, APPLY CHANGES SCD1/SCD2, materialized views).
# MAGIC Jobs & Pipelines -> insurance-lakeflow-pipeline -> table click -> **Data quality** tab.

# COMMAND ----------

display(spark.sql("""
  SELECT 'bronze' AS step, count(*) AS rows FROM insurance_lakeflow.member_charges_bronze
  UNION ALL SELECT 'silver (expectations pass)', count(*) FROM insurance_lakeflow.member_charges_silver
  UNION ALL SELECT 'quarantine (expectations fail)', count(*) FROM insurance_lakeflow.member_charges_quarantine
  UNION ALL SELECT 'deduplicated (APPLY CHANGES SCD1)', count(*) FROM insurance_lakeflow.member_charges
"""))

# COMMAND ----------

# Reconciliation: dono pipelines ka gold same hona chahiye
display(spark.sql("""
  SELECT 'lakeflow' AS pipeline, count(*) AS facts, sum(charges) AS total_charges FROM insurance_lakeflow.fact_member_charges
  UNION ALL
  SELECT 'python + dbt', count(*), sum(charges) FROM insurance_gold.fact_member_charges
"""))

# COMMAND ----------

# MAGIC %md ## 7. Unity Catalog - lineage & governance
# MAGIC * **Catalog** -> `workspace` -> `insurance_gold` -> `fact_member_charges` -> **Lineage** tab: silver -> staging -> gold graph (automatic).
# MAGIC * Schema **tags** (`domain`, `layer`, `data_classification`) aur table/column **comments** (dbt `persist_docs`) yahin dikhte hain.

# COMMAND ----------

display(spark.sql("""
  SELECT schema_name, tag_name, tag_value
  FROM system.information_schema.schema_tags
  WHERE catalog_name = 'workspace' AND schema_name LIKE 'insurance_%'
  ORDER BY schema_name, tag_name
"""))
