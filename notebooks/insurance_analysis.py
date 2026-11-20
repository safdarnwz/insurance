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
