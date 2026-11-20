{#
  FACT: grain = 1 member x 1 policy year.

  INCREMENTAL: har run me sirf naye silver records process (silver_updated_ts > max).
  KYUN: 1 TB fact ko roz poora rebuild = ghante + paisa. Incremental = minutes.
  unique_key = record_id -> rerun par duplicate nahi (idempotent).
  Strategy: Databricks = MERGE, DuckDB = delete+insert (dono same result).

  POINT-IN-TIME JOIN: member ka landing_date jis region version ki validity me
  aata hai wahi region_sk. 2026 ke records Atlanta office se, 2027 ke Miami se
  jude rahenge - region badalne se purani history "rewrite" nahi hoti.
#}
{{
    config(
        materialized='incremental',
        unique_key='record_id',
        incremental_strategy=('merge' if target.type == 'databricks' else 'delete+insert'),
        on_schema_change='fail',
    )
}}

with members as (
    select * from {{ ref('stg_member_charges') }}
    {% if is_incremental() %}
    where silver_updated_ts > (select coalesce(max(silver_updated_ts), cast('1900-01-01' as timestamp)) from {{ this }})
    {% endif %}
)

select
    m.record_id,
    m.policy_year,
    m.landing_date,
    r.region_sk,
    {{ surrogate_key(['m.age_band', 'm.sex', 'm.bmi_category', 'm.is_smoker', 'm.children']) }} as profile_key,
    m.age,
    m.bmi,
    m.children,
    cast(m.is_smoker as int)          as smoker_flag,
    m.charges,
    m.dq_warning_count,
    m.silver_updated_ts,
    {{ dbt.current_timestamp() }}     as gold_loaded_ts
from members m
left join {{ ref('dim_region') }} r
    on  m.region = r.region
    and cast(m.landing_date as timestamp) >= r.effective_from
    and cast(m.landing_date as timestamp) <  r.effective_to
