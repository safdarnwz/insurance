-- Staging: silver ka 1:1 thin layer. Sirf select/rename/cast - koi business logic nahi.
-- KYUN: source column naam badle to sirf yahan fix, baaki models untouched.
select
    record_id,
    cast(policy_year as int)        as policy_year,
    cast(landing_date as date)      as landing_date,
    age,
    age_band,
    sex,
    bmi,
    bmi_category,
    children,
    is_smoker,
    region,
    charges,
    dq_warning_count,
    _silver_updated_ts              as silver_updated_ts
from {{ source('silver', 'member_charges') }}
