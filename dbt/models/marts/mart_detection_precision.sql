-- Precision, not just recall.  A detection is a true positive if it matches
-- either the expected initial status or the expected final status (late
-- settlements are legitimately flagged before they are corrected).
with truth as (
  select rrn, expected_status, expected_initial
  from {{ ref('stg_ground_truth') }}
),
flagged as (
  select d.rrn, d.match_status, t.expected_status, t.expected_initial
  from {{ ref('stg_match_results') }} d
  left join truth t using (rrn)
  where d.match_status <> 'MATCHED'
)
select count(*) as flagged_txns,
       count(*) filter (
         where expected_status is null
            or match_status not in (expected_initial, expected_status)
       ) as false_positives,
       round(
         100.0 * (count(*) - count(*) filter (
           where expected_status is null
              or match_status not in (expected_initial, expected_status)
         )) / nullif(count(*), 0),
         2
       ) as precision_pct
from flagged
