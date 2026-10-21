-- Current state of each RRN.  The Iceberg table is upserted by the streaming jobs,
-- but equality-delete commits can briefly leave more than one row visible to a
-- snapshot; rank and keep the latest decision so every downstream model is unique.
with ranked as (
  select *,
         row_number() over (partition by rrn order by decided_at desc) as rn
  from {{ iceberg_table('recon', 'match_results') }}
)
select rrn, merchant_id, match_status,
       switch_amount, settle_amount, callback_amount,
       switch_ts, settle_ts, callback_ts, settlement_lag_s,
       decided_at,
       coalesce(decided_date, cast(decided_at as date), current_date) as decided_date
from ranked
where rn = 1
