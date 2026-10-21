select decided_date, merchant_id, count(*) as rows
from {{ ref('mart_daily_recon') }}
group by decided_date, merchant_id
having count(*) > 1
