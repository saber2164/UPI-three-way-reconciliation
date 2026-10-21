-- Airflow metadata DB
CREATE DATABASE airflow;

\c fintech
CREATE SCHEMA IF NOT EXISTS app;

CREATE TABLE app.users (
  user_id        UUID PRIMARY KEY,
  full_name      TEXT NOT NULL,
  phone          TEXT NOT NULL,
  email          TEXT,
  pan_masked     TEXT,
  dob            DATE,
  city           TEXT,
  is_minor       BOOLEAN DEFAULT FALSE,
  created_at     TIMESTAMPTZ DEFAULT now(),
  updated_at     TIMESTAMPTZ DEFAULT now()
);

-- DPDP consent artefact: purpose-specific, versioned, hash-chained (tamper-evident)
CREATE TABLE app.consents (
  consent_id     UUID PRIMARY KEY,
  user_id        UUID NOT NULL REFERENCES app.users(user_id),
  purpose        TEXT NOT NULL,          -- kyc | credit_scoring | marketing | analytics
  status         TEXT NOT NULL,          -- granted | withdrawn | expired
  notice_version TEXT NOT NULL,          -- which privacy notice text the user saw
  granted_at     TIMESTAMPTZ,
  withdrawn_at   TIMESTAMPTZ,
  expires_at     TIMESTAMPTZ,
  channel        TEXT,                   -- app | web | consent_manager
  artefact_hash  TEXT NOT NULL,          -- sha256(prev_hash || canonical payload)
  prev_hash      TEXT,
  updated_at     TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX ON app.consents(user_id, purpose);

-- Product events carrying PII, each tagged with the purpose it was collected under
CREATE TABLE app.events (
  event_id       UUID PRIMARY KEY,
  user_id        UUID NOT NULL,
  event_type     TEXT NOT NULL,
  purpose        TEXT NOT NULL,
  amount_inr     NUMERIC(14,2),
  device_id      TEXT,
  ip_address     TEXT,
  geo_city       TEXT,
  event_ts       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Data Principal Rights requests — 90-day response window under DPDP Rules
CREATE TABLE app.dpr_requests (
  request_id     UUID PRIMARY KEY,
  user_id        UUID NOT NULL,
  request_type   TEXT NOT NULL,          -- access | correct | erase
  received_at    TIMESTAMPTZ DEFAULT now(),
  due_at         TIMESTAMPTZ GENERATED ALWAYS AS (received_at + interval '90 days') STORED,
  status         TEXT NOT NULL DEFAULT 'received', -- received | in_progress | completed | rejected
  completed_at   TIMESTAMPTZ,
  evidence_uri   TEXT
);

ALTER TABLE app.users REPLICA IDENTITY FULL;
ALTER TABLE app.consents REPLICA IDENTITY FULL;
ALTER TABLE app.events REPLICA IDENTITY FULL;
ALTER TABLE app.dpr_requests REPLICA IDENTITY FULL;
CREATE PUBLICATION dbz_publication FOR TABLE app.users, app.consents, app.events, app.dpr_requests;

-- Project 2: low-latency serving tables written by Flink
CREATE SCHEMA IF NOT EXISTS recon;
CREATE TABLE recon.match_results (
  rrn              TEXT PRIMARY KEY,
  merchant_id      TEXT,
  match_status     TEXT,   -- MATCHED | AMOUNT_MISMATCH | MISSING_SETTLEMENT | MISSING_CALLBACK
  switch_amount    NUMERIC(14,2),
  settle_amount    NUMERIC(14,2),
  callback_amount  NUMERIC(14,2),
  switch_ts        TIMESTAMPTZ,
  settle_ts        TIMESTAMPTZ,
  callback_ts      TIMESTAMPTZ,
  settlement_lag_s INTEGER,
  decision_ts      TIMESTAMPTZ,   -- event time the decision is anchored to
  decided_at       TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_match_results_decided_at ON recon.match_results (decided_at DESC);
CREATE INDEX IF NOT EXISTS idx_match_results_status ON recon.match_results (match_status);

-- The serving table is monotonic in decision time: a replay of the fast job must
-- not overwrite a correction written by the repair job.
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

-- Decision history.  A late settlement produces two rows for the
-- same RRN (MISSING_SETTLEMENT then MATCHED); match_results shows current state.
-- The Flink JDBC sink upserts on (rrn, decision_ts), so that pair is unique and
-- a replay cannot invent a second history row.
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
CREATE INDEX IF NOT EXISTS idx_decision_audit_rrn ON recon.decision_audit (rrn, decision_ts);
CREATE INDEX IF NOT EXISTS idx_decision_audit_decided_at ON recon.decision_audit (decided_at DESC);

CREATE TABLE recon.merchant_minute_stats (
  merchant_id      TEXT,
  window_start     TIMESTAMPTZ,
  total_txns       INTEGER,
  matched          INTEGER,
  unmatched        INTEGER,
  PRIMARY KEY (merchant_id, window_start)
);
CREATE TABLE recon.alerts (
  alert_id         BIGSERIAL PRIMARY KEY,
  merchant_id      TEXT,
  window_start     TIMESTAMPTZ,
  unmatched_rate   NUMERIC(6,4),
  message          TEXT,
  created_at       TIMESTAMPTZ DEFAULT now(),
  UNIQUE (merchant_id, window_start)   -- the Flink JDBC sink upserts on this key
);

CREATE SCHEMA IF NOT EXISTS obs;
CREATE TABLE obs.pipeline_metrics (
  ts TIMESTAMPTZ DEFAULT now(), project TEXT, pipeline TEXT, metric TEXT, value NUMERIC, labels JSONB
);

-- Project 2 evaluation: injected faults (consumed from Kafka topic recon.ground_truth by Airflow)
CREATE TABLE recon.ground_truth (
  rrn              TEXT PRIMARY KEY,
  fault            TEXT,
  merchant_id      TEXT,
  expected_status  TEXT,
  expected_initial TEXT,
  injected_at      TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_ground_truth_fault ON recon.ground_truth (fault);

-- Existing shared volumes were initialized before these columns existed.
ALTER TABLE recon.ground_truth ADD COLUMN IF NOT EXISTS expected_status TEXT;
ALTER TABLE recon.ground_truth ADD COLUMN IF NOT EXISTS expected_initial TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS recon_alerts_merchant_window_uidx
  ON recon.alerts (merchant_id, window_start);
