-- Standalone Project 2 database.  No Airflow, no Debezium, no shared schemas:
-- this stack must also fit on a laptop with 8 GB of Docker memory.
CREATE SCHEMA IF NOT EXISTS recon;

-- Hot serving table written by the Flink JDBC upsert sink.
-- rrn is the natural key: a late settlement corrects the same row in place.
-- decision_ts is the *event* time the decision is anchored to (switch time for the
-- fast job, settlement time for the repair job).  It is deterministic, so a replay
-- cannot invent a second history row, and it orders corrections correctly.
CREATE TABLE IF NOT EXISTS recon.match_results (
  rrn              TEXT PRIMARY KEY,
  merchant_id      TEXT,
  match_status     TEXT,
  switch_amount    NUMERIC(14,2),
  settle_amount    NUMERIC(14,2),
  callback_amount  NUMERIC(14,2),
  switch_ts        TIMESTAMPTZ,
  settle_ts        TIMESTAMPTZ,
  callback_ts      TIMESTAMPTZ,
  settlement_lag_s INTEGER,
  decision_ts      TIMESTAMPTZ,
  decided_at       TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_match_results_decided_at
  ON recon.match_results (decided_at DESC);
CREATE INDEX IF NOT EXISTS idx_match_results_status
  ON recon.match_results (match_status);

-- The serving table is monotonic in decision time.  Without this, a replay of the
-- fast job could overwrite a correction written by the repair job and put an RRN
-- back to MISSING_SETTLEMENT.  A stale write is rejected, the newer decision wins.
CREATE OR REPLACE FUNCTION recon.reject_stale_decision() RETURNS trigger AS $$
BEGIN
  IF OLD.decision_ts IS NOT NULL
     AND NEW.decision_ts IS NOT NULL
     AND NEW.decision_ts < OLD.decision_ts THEN
    RETURN OLD;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_match_results_monotonic ON recon.match_results;
CREATE TRIGGER trg_match_results_monotonic
  BEFORE UPDATE ON recon.match_results
  FOR EACH ROW EXECUTE FUNCTION recon.reject_stale_decision();

-- Decision history.  A late settlement produces two rows for the same RRN
-- (MISSING_SETTLEMENT then MATCHED), which is how the demo proves the
-- correction actually happened.  match_results itself only shows current state.
-- The natural key is (rrn, decision_ts): the Flink sink upserts on it, so a
-- re-delivery of the same decision is idempotent while a genuinely new
-- decision for the same RRN becomes a new history row.
CREATE TABLE IF NOT EXISTS recon.decision_audit (
  audit_id         BIGSERIAL PRIMARY KEY,
  rrn              TEXT,
  merchant_id      TEXT,
  match_status     TEXT,
  switch_amount    NUMERIC(14,2),
  settle_amount    NUMERIC(14,2),
  callback_amount  NUMERIC(14,2),
  switch_ts        TIMESTAMPTZ,
  settle_ts        TIMESTAMPTZ,
  callback_ts      TIMESTAMPTZ,
  settlement_lag_s INTEGER,
  decision_ts      TIMESTAMPTZ NOT NULL,
  decided_at       TIMESTAMPTZ,
  UNIQUE (rrn, decision_ts)
);
CREATE INDEX IF NOT EXISTS idx_decision_audit_rrn
  ON recon.decision_audit (rrn, decision_ts);
CREATE INDEX IF NOT EXISTS idx_decision_audit_decided_at
  ON recon.decision_audit (decided_at DESC);

CREATE TABLE IF NOT EXISTS recon.merchant_minute_stats (
  merchant_id      TEXT,
  window_start     TIMESTAMPTZ,
  total_txns       INTEGER,
  matched          INTEGER,
  unmatched        INTEGER,
  PRIMARY KEY (merchant_id, window_start)
);

-- The alert sink is an upsert, so (merchant_id, window_start) must be unique.
CREATE TABLE IF NOT EXISTS recon.alerts (
  alert_id         BIGSERIAL PRIMARY KEY,
  merchant_id      TEXT,
  window_start     TIMESTAMPTZ,
  unmatched_rate   NUMERIC(6,4),
  message          TEXT,
  created_at       TIMESTAMPTZ DEFAULT now(),
  UNIQUE (merchant_id, window_start)
);

-- Injected-fault oracle consumed from recon.ground_truth by truth_sink.py.
CREATE TABLE IF NOT EXISTS recon.ground_truth (
  rrn              TEXT PRIMARY KEY,
  fault            TEXT,
  merchant_id      TEXT,
  expected_status  TEXT,
  expected_initial TEXT,
  injected_at      TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_ground_truth_fault ON recon.ground_truth (fault);
