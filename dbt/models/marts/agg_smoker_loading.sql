-- Smoker loading factor: smoker ka average cost non-smoker se kitne guna?
-- Actuarial team premium me "smoker loading" isi se decide karti hai.
with by_band as (
    select
        p.age_band,
        avg(case when f.smoker_flag = 0 then f.charges end) as avg_non_smoker,
        avg(case when f.smoker_flag = 1 then f.charges end) as avg_smoker,
        count(*)                                            as member_count
    from {{ ref('fact_member_charges') }} f
    join {{ ref('dim_member_profile') }} p on f.profile_key = p.profile_key
    group by 1
)
select
    age_band,
    round(avg_non_smoker, 2)                                   as avg_charges_non_smoker,
    round(avg_smoker, 2)                                       as avg_charges_smoker,
    member_count,
    -- nullif: divide-by-zero se bachav (Databricks ANSI mode me error deta hai)
    round(avg_smoker / nullif(avg_non_smoker, 0), 2)           as smoker_loading_factor
from by_band
