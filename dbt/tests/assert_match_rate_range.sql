select *
from {{ ref('mart_daily_recon') }}
where match_rate_pct < 0 or match_rate_pct > 100
