-- Segment-wise cost: kaun sa segment kitna kharcha karta hai (pricing / underwriting input)
select
    f.policy_year,
    r.region,
    p.smoker_status,
    p.bmi_category,
    count(*)                                   as member_count,
    sum(f.charges)                             as total_charges,
    round(avg(f.charges), 2)                   as avg_charges,
    round({{ percentile('f.charges', 0.5) }}, 2) as median_charges,
    round({{ percentile('f.charges', 0.9) }}, 2) as p90_charges
from {{ ref('fact_member_charges') }} f
join {{ ref('dim_member_profile') }} p on f.profile_key = p.profile_key
join {{ ref('dim_region') }} r          on f.region_sk  = r.region_sk
group by 1, 2, 3, 4
