select
    region,
    region_name,
    country,
    regional_office,
    risk_zone,
    cast(effective_date as timestamp) as effective_ts
from {{ source('silver', 'region_master') }}
