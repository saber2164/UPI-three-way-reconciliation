-- Control total: every rupee of switch value must land in exactly one bucket.
-- If this returns rows, the finance totals are lying to someone.
with x as (
  select decided_date,
         merchant_id,
         gross_inr,
         reconciled_inr + mismatch_inr + outstanding_inr + missing_callback_inr as accounted_inr
  from {{ ref('mart_daily_recon') }}
)
select *
from x
where abs(gross_inr - accounted_inr) > 0.01
