-- The staging model deduplicates; this catches regressions in that logic.
select rrn, count(*) as rows
from {{ ref('stg_match_results') }}
group by rrn
having count(*) > 1
