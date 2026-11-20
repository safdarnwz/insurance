{#
  SCD TYPE 2 via dbt snapshot.
  Har run me dbt current data ko snapshot table se compare karta hai:
    - koi change nahi  -> kuch nahi
    - value badli      -> purani row ka dbt_valid_to set (expire) + nayi row insert
  strategy=timestamp: `effective_ts` (silver me sirf asli change par badalta hai)
  -> version ki validity BUSINESS date se, run time se nahi. Isliye full-refresh /
  backfill par bhi same history banti hai.

  KYA NAHI KARNA: snapshot table ko kabhi drop / full-refresh mat karo - history
  sirf yahin hai (silver SCD1 sirf latest rakhta hai).
#}
{% snapshot snap_region_master %}
{{
    config(
        unique_key='region',
        strategy='timestamp',
        updated_at='effective_ts',
        invalidate_hard_deletes=True,
    )
}}
select * from {{ ref('stg_region_master') }}
{% endsnapshot %}
