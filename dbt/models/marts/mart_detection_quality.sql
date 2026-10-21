-- Did the streaming job catch what was injected?
--
-- initial_recall: status the fast bounded job should emit first.  For
-- LATE_SETTLE this is MISSING_SETTLEMENT by design.
-- final_recall: status after the late-correction job has run.
with truth as (
  select rrn, fault, expected_status, expected_initial
  from {{ ref('stg_ground_truth') }}
),
det as (
  select rrn, match_status, settle_amount
  from {{ ref('stg_match_results') }}
)
select t.fault,
       count(*) as injected,
       sum(case when d.match_status = t.expected_initial then 1 else 0 end) as initial_ok,
       round(100.0 * sum(case when d.match_status = t.expected_initial then 1 else 0 end) / count(*), 2) as initial_recall_pct,
       sum(case when d.match_status = t.expected_status then 1 else 0 end) as final_ok,
       round(100.0 * sum(case when d.match_status = t.expected_status then 1 else 0 end) / count(*), 2) as final_recall_pct,
       sum(case when t.fault = 'LATE_SETTLE' and d.settle_amount is not null then 1 else 0 end) as late_corrected
from truth t
left join det d using (rrn)
group by 1
order by 1
