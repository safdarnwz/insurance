-- SCD2 integrity: har region ki EXACTLY ek current version honi chahiye.
-- 0 rows = pass. Koi row aayi = snapshot/dim logic me bug.
select region, count(*) as current_versions
from {{ ref('dim_region') }}
where is_current
group by region
having count(*) <> 1
