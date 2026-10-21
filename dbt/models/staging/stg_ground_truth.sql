-- Injected-fault oracle, written by truth_sink.py (standalone) or the Airflow
-- consumer (shared stack).  Used only for evaluation, never by the pipeline.
select rrn, fault, merchant_id, expected_status, expected_initial, injected_at
from pg.recon.ground_truth
