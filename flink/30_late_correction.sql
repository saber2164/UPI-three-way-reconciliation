-- =========================================================================================
-- Late-settlement correction job.
--
-- The fast job bounds settlement state at 4 h (scaled) to keep state small and to surface
-- MISSING_SETTLEMENT quickly.  A settlement that arrives after that bound would otherwise
-- be invisible forever.  This second job keeps a longer (24 h scaled) switch+callback state
-- and joins *only* settlements in the late window.  It upserts the corrected decision into
-- Postgres and Iceberg, and appends the correction to decision_audit.
--
-- As in the fast job, the interval join reads the raw append-only streams and the RRN
-- dedup happens on the joined result: a Flink interval join cannot consume the retracting
-- output of ROW_NUMBER() dedup.
--
-- This is the explicit trade-off: a bounded fast path for latency, plus a longer-state
-- repair path for completeness.  In production you would also alert on correction volume.
-- =========================================================================================

-- ---------------------------------------------------------------------------------------
-- Own copies of the sources.  Two Flink jobs reading the same topic with the same
-- consumer group id join the *same* Kafka consumer group, and the group coordinator
-- hands each partition to exactly one member: the two jobs would split the partitions
-- and the repair job would silently see nothing.  Separate group ids per job keep both
-- readers complete.
-- ---------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS switch_txn_late (
  rrn STRING, txn_id STRING, merchant_id STRING, payer_vpa STRING, payee_vpa STRING,
  amount_paise BIGINT, status STRING, resp_code STRING,
  event_ts BIGINT,
  event_ts_ltz AS TO_TIMESTAMP_LTZ(event_ts, 3),
  WATERMARK FOR event_ts_ltz AS event_ts_ltz - __WM_SWITCH__
) WITH (
  'connector' = 'kafka',
  'topic' = 'upi.switch',
  'properties.bootstrap.servers' = '__KAFKA_BOOTSTRAP__',
  'properties.group.id' = 'recon-switch-late',
  'scan.startup.mode' = 'earliest-offset',
  'properties.auto.offset.reset' = 'earliest',
  'format' = 'avro-confluent',
  'avro-confluent.url' = '__SCHEMA_REGISTRY__'
);

CREATE TABLE IF NOT EXISTS settlement_late (
  rrn STRING, utr STRING, merchant_id STRING, amount_paise BIGINT,
  settlement_cycle STRING, bank_ref STRING,
  event_ts BIGINT,
  event_ts_ltz AS TO_TIMESTAMP_LTZ(event_ts, 3),
  WATERMARK FOR event_ts_ltz AS event_ts_ltz - __WM_SETTLE__
) WITH (
  'connector' = 'kafka',
  'topic' = 'upi.settlements',
  'properties.bootstrap.servers' = '__KAFKA_BOOTSTRAP__',
  'properties.group.id' = 'recon-settle-late',
  'scan.startup.mode' = 'earliest-offset',
  'properties.auto.offset.reset' = 'earliest',
  'format' = 'avro-confluent',
  'avro-confluent.url' = '__SCHEMA_REGISTRY__'
);

CREATE TABLE IF NOT EXISTS callback_late (
  rrn STRING, merchant_id STRING, order_id STRING, amount_paise BIGINT,
  ack_status STRING,
  event_ts BIGINT,
  event_ts_ltz AS TO_TIMESTAMP_LTZ(event_ts, 3),
  WATERMARK FOR event_ts_ltz AS event_ts_ltz - __WM_CALLBACK__
) WITH (
  'connector' = 'kafka',
  'topic' = 'upi.callbacks',
  'properties.bootstrap.servers' = '__KAFKA_BOOTSTRAP__',
  'properties.group.id' = 'recon-callback-late',
  'scan.startup.mode' = 'earliest-offset',
  'properties.auto.offset.reset' = 'earliest',
  'format' = 'avro-confluent',
  'avro-confluent.url' = '__SCHEMA_REGISTRY__'
);

CREATE TEMPORARY VIEW late_joined AS
SELECT
  s.rrn, s.merchant_id,
  s.amount_paise AS sw_amt,
  st.amount_paise AS st_amt,
  c.amount_paise AS cb_amt,
  s.event_ts_ltz AS switch_ts,
  st.event_ts_ltz AS settle_ts,
  c.event_ts_ltz AS callback_ts,
  c.ack_status
FROM switch_txn_late s
LEFT JOIN callback_late c
  ON s.rrn = c.rrn
 AND c.event_ts_ltz BETWEEN s.event_ts_ltz - __CB_WINDOW__ AND s.event_ts_ltz + __CB_WINDOW__
INNER JOIN settlement_late st
  ON s.rrn = st.rrn
 AND st.event_ts_ltz BETWEEN s.event_ts_ltz + __SETTLE_WINDOW__
                         AND s.event_ts_ltz + __LATE_WINDOW__
WHERE s.status = 'SUCCESS';

CREATE TEMPORARY VIEW late_decided AS
SELECT
  rrn, merchant_id,
  CASE
    WHEN cb_amt IS NULL OR ack_status <> 'ACK' THEN 'MISSING_CALLBACK'
    WHEN sw_amt <> st_amt OR sw_amt <> cb_amt    THEN 'AMOUNT_MISMATCH'
    ELSE 'MATCHED'
  END AS match_status,
  CAST(CAST(sw_amt AS DECIMAL(18,2)) / 100 AS DECIMAL(14,2)) AS switch_amount,
  CAST(CAST(st_amt AS DECIMAL(18,2)) / 100 AS DECIMAL(14,2)) AS settle_amount,
  CAST(CAST(cb_amt AS DECIMAL(18,2)) / 100 AS DECIMAL(14,2)) AS callback_amount,
  switch_ts,
  -- Only switch_ts stays a rowtime attribute (a sink accepts one event-time column).
  CAST(settle_ts AS TIMESTAMP_LTZ(3)) AS settle_ts,
  CAST(callback_ts AS TIMESTAMP_LTZ(3)) AS callback_ts,
  CAST(TIMESTAMPDIFF(SECOND, switch_ts, settle_ts) AS INT) AS settlement_lag_s,
  -- The correction is anchored to the late settlement, so it gets its own decision key
  -- and the audit shows the correction *after* the original MISSING_SETTLEMENT.
  CAST(settle_ts AS TIMESTAMP(3)) AS decision_ts,
  CURRENT_TIMESTAMP AS decided_at
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY rrn ORDER BY switch_ts ASC) AS rn
  FROM late_joined
)
WHERE rn = 1;

SET 'pipeline.name' = 'upi-recon-late-correction';

EXECUTE STATEMENT SET
BEGIN
  INSERT INTO pg_match_results
  SELECT rrn, merchant_id, match_status, switch_amount, settle_amount, callback_amount,
         CAST(switch_ts AS TIMESTAMP(3)), CAST(settle_ts AS TIMESTAMP(3)),
         CAST(callback_ts AS TIMESTAMP(3)), settlement_lag_s,
         decision_ts, CAST(decided_at AS TIMESTAMP(3))
  FROM late_decided;

  INSERT INTO pg_decision_audit
  SELECT rrn, merchant_id, match_status, switch_amount, settle_amount, callback_amount,
         CAST(switch_ts AS TIMESTAMP(3)), CAST(settle_ts AS TIMESTAMP(3)),
         CAST(callback_ts AS TIMESTAMP(3)), settlement_lag_s,
         decision_ts, CAST(decided_at AS TIMESTAMP(3))
  FROM late_decided;

  INSERT INTO lake.recon.match_results
  SELECT rrn, merchant_id, match_status, switch_amount, settle_amount, callback_amount,
         switch_ts, settle_ts, callback_ts, settlement_lag_s, decided_at,
         CAST(decided_at AS DATE) AS decided_date
  FROM late_decided;
END;
