# ELT ya ETL? — Ye project **ELT** hai

## Farq ek line me

| | ETL (Extract → Transform → Load) | ELT (Extract → Load → Transform) |
|---|---|---|
| Transform kahan | Load **se pehle**, alag ETL server/tool par (Informatica, SSIS, Python script) | Load **ke baad**, target platform ke andar (Spark/Databricks, Snowflake, BigQuery, DuckDB) |
| Raw data | Aksar save hi nahi hota – sirf transformed data warehouse me | Raw data pehle as-is save (Bronze) |
| Logic badla to | Source se dobara extract karna padta hai | Bronze se dobara transform kar lo (replay) |
| Scale | ETL server ki capacity tak | Lakehouse/warehouse ka distributed compute |
| Kab sahi | Sensitive data load se pehle mask karna zaroori ho, chhota fixed data, legacy warehouses | Modern cloud/lakehouse, bada data, badalti requirements |

## Is project me ELT kaise hai

```
E  Extract   : Kaggle API / sample file  ──────────────────────────────►  landing/  (as-is file + manifest)
L  Load      : Spark Auto Loader / streaming  ─────────────────────────►  BRONZE    (raw, sab STRING, koi logic nahi)
T  Transform : Spark (heavy, row-level, TB scale) ─────────────────────►  SILVER    (clean, typed, DQ, dedupe)
T  Transform : dbt SQL (business modelling) ───────────────────────────►  GOLD      (star schema, SCD2, KPIs)
```

**Kyun ELT chuna:**
1. **Replay:** Silver/gold me bug mila → bronze se dobara bana lo. Source system ko dobara pareshan nahi karna.
2. **Audit:** Bronze me source ka exact data + file name + time. "Ye number kahan se aaya?" ka jawab hamesha milta hai.
3. **Scale:** Transform lakehouse ke andar (Spark/Databricks) – 1 TB bhi parallel me.
4. **dbt:** "T" ko SQL me, version-controlled, tested aur documented rakhta hai – dbt ELT ka standard tool hai.

**Kya nahi karna:** Bronze me hi cleaning/casting shuru kar dena ("thoda sa transform kar dete hain") – fir ye na ETL rahta hai na ELT, aur raw data ka proof kho jaata hai.

## Ek nuance (interview me poocha jaata hai)

Silver step technically "Spark me transform" hai, warehouse me nahi. Phir bhi ye ELT hai kyunki transform **load ke baad, lakehouse platform ke andar** ho raha hai – lakehouse hi aapka warehouse hai. Medallion architecture = ELT ka lakehouse version.
