-- SCD2 integrity: ek region ke do versions ki validity periods overlap nahi honi chahiye,
-- warna fact ka point-in-time join ek row ko 2 versions se jod dega (double counting).
select a.region, a.region_sk, b.region_sk as overlapping_sk
from {{ ref('dim_region') }} a
join {{ ref('dim_region') }} b
  on a.region = b.region
 and a.region_sk <> b.region_sk
 and a.effective_from < b.effective_to
 and b.effective_from < a.effective_to
