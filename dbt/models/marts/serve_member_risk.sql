-- SERVING MART: app / API lookups ke liye (Lakebase Postgres me synced table banta hai).
-- KYUN alag model: BI ko star schema chahiye (joins), lekin app ko ek denormalized
-- row chahiye jo primary key se milliseconds me mile ("is member ka risk kya hai?").
-- Joins yahan (lakehouse me) ek baar, app me har request par nahi.
select
    f.record_id,
    f.policy_year,
    f.age,
    p.sex,
    f.bmi,
    p.bmi_category,
    f.children,
    p.smoker_status,
    p.risk_segment,
    r.region,
    r.regional_office,
    r.risk_zone,
    f.charges,
    -- 1.0 = us saal ka average member; 2.5 = average se 2.5x mehenga
    round(f.charges / nullif(avg(f.charges) over (partition by f.policy_year), 0), 2) as cost_index
from {{ ref('fact_member_charges') }} f
join {{ ref('dim_member_profile') }} p on f.profile_key = p.profile_key
join {{ ref('dim_region') }} r          on f.region_sk  = r.region_sk
