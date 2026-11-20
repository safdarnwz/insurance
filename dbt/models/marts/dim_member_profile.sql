-- "Junk dimension": low-cardinality attributes ke combinations (age_band x sex x
-- bmi_category x smoker x children). Fact me 5 columns ki jagah 1 key -> clean
-- BI slicers, chhota fact.
with profiles as (
    select distinct age_band, sex, bmi_category, is_smoker, children
    from {{ ref('stg_member_charges') }}
)
select
    {{ surrogate_key(['age_band', 'sex', 'bmi_category', 'is_smoker', 'children']) }} as profile_key,
    age_band,
    sex,
    bmi_category,
    is_smoker,
    case when is_smoker then 'Smoker' else 'Non-Smoker' end as smoker_status,
    children,
    -- Underwriting risk segment: business rule EK jagah (gold), har report me nahi
    case
        when is_smoker and bmi_category = 'Obese' then 'Very High'
        when is_smoker                           then 'High'
        when bmi_category = 'Obese'              then 'Medium'
        else 'Low'
    end as risk_segment
from profiles
