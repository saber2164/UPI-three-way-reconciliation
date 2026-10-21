-- Finance view: per settlement day and merchant, how much is reconciled,
-- mismatched, outstanding, and missing a callback.
--
-- Control total identity:
--   gross = reconciled + mismatch + outstanding + missing_callback
-- `mismatch_inr` is the *full switch value* of amount mismatches, not the delta.
-- Using the delta would make the control total look balanced while money is
-- still unresolved; `mismatch_delta_inr` is the ops investigation amount.
select decided_date,
       merchant_id,
       count(*) as txns,
       coalesce(sum(switch_amount), 0) as gross_inr,
       coalesce(sum(case when match_status = 'MATCHED' then switch_amount end), 0) as reconciled_inr,
       coalesce(sum(case when match_status = 'AMOUNT_MISMATCH' then switch_amount end), 0) as mismatch_inr,
       coalesce(sum(case when match_status = 'AMOUNT_MISMATCH'
                         then abs(switch_amount - coalesce(settle_amount, 0)) end), 0) as mismatch_delta_inr,
       coalesce(sum(case when match_status = 'MISSING_SETTLEMENT' then switch_amount end), 0) as outstanding_inr,
       coalesce(sum(case when match_status = 'MISSING_CALLBACK' then switch_amount end), 0) as missing_callback_inr,
       round(100.0 * sum(case when match_status = 'MATCHED' then 1 else 0 end) / count(*), 2) as match_rate_pct,
       quantile_cont(settlement_lag_s, 0.5) as p50_lag_s,
       quantile_cont(settlement_lag_s, 0.99) as p99_lag_s
from {{ ref('stg_match_results') }}
group by 1, 2
