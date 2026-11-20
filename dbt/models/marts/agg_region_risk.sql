-- Region risk scorecard (current office/risk zone ke naam ke saath)
with totals as (
    select sum(charges) as grand_total from {{ ref('fact_member_charges') }}
),
by_region as (
    select
        r.region,
        count(*)                                                        as member_count,
        sum(f.charges)                                                  as total_charges,
        round(avg(f.charges), 2)                                        as avg_charges,
        round(avg(f.smoker_flag) * 100, 2)                              as smoker_pct,
        round(avg(case when p.bmi_category = 'Obese' then 1 else 0 end) * 100, 2) as obese_pct
    from {{ ref('fact_member_charges') }} f
    join {{ ref('dim_region') }} r          on f.region_sk  = r.region_sk
    join {{ ref('dim_member_profile') }} p  on f.profile_key = p.profile_key
    group by 1
)
select
    b.*,
    round(b.total_charges * 100 / nullif(t.grand_total, 0), 2)  as share_of_total_charges_pct,
    c.regional_office                                            as current_office,
    c.risk_zone                                                  as current_risk_zone
from by_region b
cross join totals t
left join {{ ref('dim_region') }} c on c.region = b.region and c.is_current
