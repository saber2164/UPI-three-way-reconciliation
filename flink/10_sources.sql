-- =========================================================================================
-- Sources.  ``event_ts`` is epoch millis on the wire; ``event_ts_ltz`` is the event-time
-- attribute used by watermarks and interval joins.  Keeping the raw long makes the Avro
-- contract independent of Flink's logical-type mapping.
--
-- Watermarks are intentionally *tight* in event time and rendered from TIME_SCALE:
--   switch/callback out-of-orderness : 30 s / TIME_SCALE
--   settlement out-of-orderness       : 5 min / TIME_SCALE
-- =========================================================================================

CREATE TABLE IF NOT EXISTS switch_txn (
  rrn STRING, txn_id STRING, merchant_id STRING, payer_vpa STRING, payee_vpa STRING,
  amount_paise BIGINT, status STRING, resp_code STRING,
  event_ts BIGINT,
  event_ts_ltz AS TO_TIMESTAMP_LTZ(event_ts, 3),
  WATERMARK FOR event_ts_ltz AS event_ts_ltz - __WM_SWITCH__
) WITH (
  'connector' = 'kafka',
  'topic' = 'upi.switch',
  'properties.bootstrap.servers' = '__KAFKA_BOOTSTRAP__',
  'properties.group.id' = 'recon-switch',
  'scan.startup.mode' = 'earliest-offset',
  'properties.auto.offset.reset' = 'earliest',
  'format' = 'avro-confluent',
  'avro-confluent.url' = '__SCHEMA_REGISTRY__'
);

CREATE TABLE IF NOT EXISTS settlement (
  rrn STRING, utr STRING, merchant_id STRING, amount_paise BIGINT,
  settlement_cycle STRING, bank_ref STRING,
  event_ts BIGINT,
  event_ts_ltz AS TO_TIMESTAMP_LTZ(event_ts, 3),
  WATERMARK FOR event_ts_ltz AS event_ts_ltz - __WM_SETTLE__
) WITH (
  'connector' = 'kafka',
  'topic' = 'upi.settlements',
  'properties.bootstrap.servers' = '__KAFKA_BOOTSTRAP__',
  'properties.group.id' = 'recon-settle',
  'scan.startup.mode' = 'earliest-offset',
  'properties.auto.offset.reset' = 'earliest',
  'format' = 'avro-confluent',
  'avro-confluent.url' = '__SCHEMA_REGISTRY__'
);

CREATE TABLE IF NOT EXISTS callback (
  rrn STRING, merchant_id STRING, order_id STRING, amount_paise BIGINT,
  ack_status STRING,
  event_ts BIGINT,
  event_ts_ltz AS TO_TIMESTAMP_LTZ(event_ts, 3),
  WATERMARK FOR event_ts_ltz AS event_ts_ltz - __WM_CALLBACK__
) WITH (
  'connector' = 'kafka',
  'topic' = 'upi.callbacks',
  'properties.bootstrap.servers' = '__KAFKA_BOOTSTRAP__',
  'properties.group.id' = 'recon-callback',
  'scan.startup.mode' = 'earliest-offset',
  'properties.auto.offset.reset' = 'earliest',
  'format' = 'avro-confluent',
  'avro-confluent.url' = '__SCHEMA_REGISTRY__'
);

-- ---------------------------------------------------------------------------------------
-- Hot serving sinks.  The JDBC connector needs a primary key to do an upsert; match_results
-- is keyed by RRN so a late settlement can correct a previously emitted exception.
--
-- Timestamps are plain TIMESTAMP(3), not TIMESTAMP_LTZ(3): the Postgres row converter of
-- flink-connector-jdbc 3.2.0 rejects TIMESTAMP_LTZ ("Unsupported type").  The session time
-- zone is UTC in the container, so the stored values stay comparable with the event times.
-- ---------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pg_match_results (
  rrn STRING, merchant_id STRING, match_status STRING,
  switch_amount DECIMAL(14,2), settle_amount DECIMAL(14,2), callback_amount DECIMAL(14,2),
  switch_ts TIMESTAMP(3), settle_ts TIMESTAMP(3), callback_ts TIMESTAMP(3),
  settlement_lag_s INT, decision_ts TIMESTAMP(3), decided_at TIMESTAMP(3),
  PRIMARY KEY (rrn) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = '__POSTGRES_JDBC__',
  'table-name' = 'recon.match_results',
  'username' = '__POSTGRES_USER__',
  'password' = '__POSTGRES_PASSWORD__',
  'sink.buffer-flush.max-rows' = '200',
  'sink.buffer-flush.interval' = '1s',
  'sink.max-retries' = '3'
);

-- Decision history.  Two rows for one RRN mean a correction happened: the fast job
-- recorded MISSING_SETTLEMENT and the late-correction job recorded MATCHED.  The
-- natural key is (rrn, decided_at): every decision is stamped with the processing
-- time it was taken, so a later decision for the same RRN is a new history row
-- while a re-delivery of the same decision is idempotent.
CREATE TABLE IF NOT EXISTS pg_decision_audit (
  rrn STRING, merchant_id STRING, match_status STRING,
  switch_amount DECIMAL(14,2), settle_amount DECIMAL(14,2), callback_amount DECIMAL(14,2),
  switch_ts TIMESTAMP(3), settle_ts TIMESTAMP(3), callback_ts TIMESTAMP(3),
  settlement_lag_s INT, decision_ts TIMESTAMP(3), decided_at TIMESTAMP(3),
  PRIMARY KEY (rrn, decision_ts) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = '__POSTGRES_JDBC__',
  'table-name' = 'recon.decision_audit',
  'username' = '__POSTGRES_USER__',
  'password' = '__POSTGRES_PASSWORD__',
  'sink.buffer-flush.max-rows' = '500',
  'sink.buffer-flush.interval' = '1s',
  'sink.max-retries' = '3'
);

CREATE TABLE IF NOT EXISTS pg_merchant_minute (
  merchant_id STRING, window_start TIMESTAMP(3),
  total_txns INT, matched INT, unmatched INT,
  PRIMARY KEY (merchant_id, window_start) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = '__POSTGRES_JDBC__',
  'table-name' = 'recon.merchant_minute_stats',
  'username' = '__POSTGRES_USER__',
  'password' = '__POSTGRES_PASSWORD__',
  'sink.buffer-flush.max-rows' = '200',
  'sink.buffer-flush.interval' = '2s',
  'sink.max-retries' = '3'
);

CREATE TABLE IF NOT EXISTS pg_alerts (
  merchant_id STRING, window_start TIMESTAMP(3),
  unmatched_rate DECIMAL(6,4), message STRING,
  PRIMARY KEY (merchant_id, window_start) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = '__POSTGRES_JDBC__',
  'table-name' = 'recon.alerts',
  'username' = '__POSTGRES_USER__',
  'password' = '__POSTGRES_PASSWORD__',
  'sink.buffer-flush.max-rows' = '20',
  'sink.buffer-flush.interval' = '1s',
  'sink.max-retries' = '3'
);

-- ---------------------------------------------------------------------------------------
-- Durable store.  This is an append-only *decision log*: every decision the pipeline
-- takes is written once, partitioned by decision day.  The current state per RRN lives
-- in Postgres (keyed by RRN, monotonic in decision time); the log is the replayable,
-- audit-grade copy.
--
-- Iceberg equality deletes are deliberately switched off.  DuckDB's iceberg_scan
-- materialises the equality-delete index for every data file, and on this table that
-- needs more than 2 GB of RAM for 9 MB of Parquet - it does not fit next to the
-- streaming stack on an 8 GB laptop.  Without deletes the same scan costs 0.3 s and
-- 110 MB, and the dbt staging model applies the "latest decision per RRN" rule that
-- the deletes would have encoded.
-- ---------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lake.recon.match_results (
  rrn STRING, merchant_id STRING, match_status STRING,
  switch_amount DECIMAL(14,2), settle_amount DECIMAL(14,2), callback_amount DECIMAL(14,2),
  switch_ts TIMESTAMP_LTZ(3), settle_ts TIMESTAMP_LTZ(3), callback_ts TIMESTAMP_LTZ(3),
  settlement_lag_s INT, decided_at TIMESTAMP_LTZ(3), decided_date DATE,
  PRIMARY KEY (rrn, decided_date) NOT ENFORCED
) PARTITIONED BY (decided_date) WITH (
  'format-version' = '2',
  'write.upsert.enabled' = 'false'
);
