-- Region dimension, SCD Type 2 (dbt snapshot se).
-- Pehli version ka effective_from = 1900-01-01: KYUN - master data system me
-- baad me aaya ho to bhi purane facts ko ek region version mil jaaye (orphan na banein).
with snap as (
    select
        *,
        row_number() over (partition by region order by dbt_valid_from) as version_no
    from {{ ref('snap_region_master') }}
)
select
    {{ surrogate_key(['region', 'dbt_valid_from']) }}                     as region_sk,
    region,
    region_name,
    country,
    regional_office,
    risk_zone,
    version_no,
    case when version_no = 1 then cast('1900-01-01' as timestamp)
         else dbt_valid_from end                                            as effective_from,
    coalesce(dbt_valid_to, cast('9999-12-31' as timestamp))                 as effective_to,
    dbt_valid_to is null                                                    as is_current
from snap
