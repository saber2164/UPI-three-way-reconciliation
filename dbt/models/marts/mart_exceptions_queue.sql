-- Work queue for the ops team: every current exception with an ageing bucket
-- and a suggested action.  switch_ts is a simulated timestamp in the demo.
select rrn,
       merchant_id,
       match_status,
       switch_amount,
       settle_amount,
       callback_amount,
       switch_ts,
       date_diff('hour', switch_ts, now()) as age_hours,
       case
         when match_status = 'MISSING_CALLBACK'
           then 'Re-fire merchant webhook; if still NACK after 3 tries, initiate refund'
         when match_status = 'AMOUNT_MISMATCH'
           then 'Check MDR/fee deduction; if unexplained, raise chargeback with bank_ref'
         when match_status = 'MISSING_SETTLEMENT' and date_diff('hour', switch_ts, now()) > 24
           then 'Raise UDIR dispute with NPCI (T+1 breached)'
         when match_status = 'MISSING_SETTLEMENT'
           then 'Wait for next settlement cycle'
         else 'Investigate manually'
       end as suggested_action
from {{ ref('stg_match_results') }}
where match_status <> 'MATCHED'
