-- =========================================================================================
-- Three-way reconciliation: the fast pass.
--
-- Stream semantics worth explaining in an interview:
--  * Bounded interval joins.  Callback: +/- 2 min (scaled) so clock-skewed webhooks that
--    arrive *before* the switch are still matched.  Settlement: [0, +4 h] (scaled).
--  * LEFT JOIN + watermark.  When the settlement watermark passes switch_ts + 4 h and no
--    settlement exists, Flink emits the row with settle_amount NULL -> MISSING_SETTLEMENT.
--    No custom timers, no state store to operate.
--  * Dedup on RRN *after* the join.  This ordering is not cosmetic: a Flink interval join
--    can only consume an append-only input, while ROW_NUMBER() dedup is a retracting
--    (updating) operator.  Joining the raw Kafka streams first and deduplicating the
--    decision rows afterwards keeps the join legal and still collapses a switch retry
--    into a single exception.  The retry is a byte-identical copy, so the dedup never
--    has to retract a row it already emitted.
--  * Processing-time windows for operational KPIs.  A MISSING_SETTLEMENT decision is
--    emitted late in event time; an event-time window would attribute it to the past.
--    Operations care when the decision was made, not when the switch happened.
--  * Amounts are compared in paise.  DECIMAL is presentation only.
--
-- The one deliberate trade-off: a settlement later than the 4 h bound is *not* seen by
-- this job.  It surfaces as outstanding and is corrected by 30_late_correction.sql.
-- =========================================================================================

CREATE TEMPORARY VIEW joined AS
SELECT
  s.rrn, s.merchant_id,
  s.amount_paise AS sw_amt,
  st.amount_paise AS st_amt,
  c.amount_paise AS cb_amt,
  s.event_ts_ltz AS switch_ts,
  st.event_ts_ltz AS settle_ts,
  c.event_ts_ltz AS callback_ts,
  c.ack_status
FROM switch_txn s
LEFT JOIN callback c
  ON s.rrn = c.rrn
 AND c.event_ts_ltz BETWEEN s.event_ts_ltz - __CB_WINDOW__ AND s.event_ts_ltz + __CB_WINDOW__
LEFT JOIN settlement st
  ON s.rrn = st.rrn
 AND st.event_ts_ltz BETWEEN s.event_ts_ltz AND s.event_ts_ltz + __SETTLE_WINDOW__
WHERE s.status = 'SUCCESS';

-- One decision row per RRN.  A switch retry (same RRN, identical record) must not become
-- a second exception and must not inflate the finance totals.
CREATE TEMPORARY VIEW decided AS
SELECT
  rrn, merchant_id,
  CASE
    WHEN st_amt IS NULL                                  THEN 'MISSING_SETTLEMENT'
    WHEN cb_amt IS NULL OR ack_status <> 'ACK'            THEN 'MISSING_CALLBACK'
    WHEN sw_amt <> st_amt OR sw_amt <> cb_amt             THEN 'AMOUNT_MISMATCH'
    ELSE 'MATCHED'
  END AS match_status,
  CAST(CAST(sw_amt AS DECIMAL(18,2)) / 100 AS DECIMAL(14,2)) AS switch_amount,
  CAST(CAST(st_amt AS DECIMAL(18,2)) / 100 AS DECIMAL(14,2)) AS settle_amount,
  CAST(CAST(cb_amt AS DECIMAL(18,2)) / 100 AS DECIMAL(14,2)) AS callback_amount,
  switch_ts,
  -- Only switch_ts stays a rowtime attribute.  A sink accepts at most one event-time
  -- column, so the settlement and callback timestamps are projected as plain values.
  CAST(settle_ts AS TIMESTAMP_LTZ(3)) AS settle_ts,
  CAST(callback_ts AS TIMESTAMP_LTZ(3)) AS callback_ts,
  CAST(TIMESTAMPDIFF(SECOND, switch_ts, settle_ts) AS INT) AS settlement_lag_s,
  -- Event time the decision is anchored to (the switch).  It is deterministic, so a
  -- replayed row maps to the same audit key and cannot inflate the decision history.
  CAST(switch_ts AS TIMESTAMP(3)) AS decision_ts,
  CURRENT_TIMESTAMP AS decided_at,
  PROCTIME() AS proc_time
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY rrn ORDER BY switch_ts ASC) AS rn
  FROM joined
)
WHERE rn = 1;

SET 'pipeline.name' = 'upi-recon-main';

EXECUTE STATEMENT SET
BEGIN
  -- Current state: upsert by RRN so a later correction replaces the exception.
  INSERT INTO pg_match_results
  SELECT rrn, merchant_id, match_status, switch_amount, settle_amount, callback_amount,
         CAST(switch_ts AS TIMESTAMP(3)), CAST(settle_ts AS TIMESTAMP(3)),
         CAST(callback_ts AS TIMESTAMP(3)), settlement_lag_s,
         decision_ts, CAST(decided_at AS TIMESTAMP(3))
  FROM decided;

  -- Decision history: every decision, including the initial MISSING_SETTLEMENT.
  INSERT INTO pg_decision_audit
  SELECT rrn, merchant_id, match_status, switch_amount, settle_amount, callback_amount,
         CAST(switch_ts AS TIMESTAMP(3)), CAST(settle_ts AS TIMESTAMP(3)),
         CAST(callback_ts AS TIMESTAMP(3)), settlement_lag_s,
         decision_ts, CAST(decided_at AS TIMESTAMP(3))
  FROM decided;

  -- Durable store keeps the zone-aware timestamps.
  INSERT INTO lake.recon.match_results
  SELECT rrn, merchant_id, match_status, switch_amount, settle_amount, callback_amount,
         switch_ts, settle_ts, callback_ts, settlement_lag_s, decided_at,
         CAST(decided_at AS DATE) AS decided_date
  FROM decided;

  -- 1-minute per-merchant health (processing time).
  INSERT INTO pg_merchant_minute
  SELECT merchant_id, CAST(window_start AS TIMESTAMP(3)),
         CAST(COUNT(*) AS INT) AS total_txns,
         CAST(SUM(CASE WHEN match_status = 'MATCHED' THEN 1 ELSE 0 END) AS INT) AS matched,
         CAST(SUM(CASE WHEN match_status <> 'MATCHED' THEN 1 ELSE 0 END) AS INT) AS unmatched
  FROM TABLE(TUMBLE(TABLE decided, DESCRIPTOR(proc_time), __KPI_WINDOW__))
  GROUP BY merchant_id, window_start, window_end;

  -- Merchant-level alert: >= 10% unmatched in a 2-minute window with at least 20 txns.
  INSERT INTO pg_alerts
  SELECT merchant_id, CAST(window_start AS TIMESTAMP(3)),
         CAST(CAST(SUM(CASE WHEN match_status <> 'MATCHED' THEN 1 ELSE 0 END) AS DOUBLE)
              / COUNT(*) AS DECIMAL(6,4)) AS unmatched_rate,
         CONCAT('merchant ', merchant_id, ' unmatched rate breached in window starting ',
                CAST(window_start AS STRING)) AS message
  FROM TABLE(TUMBLE(TABLE decided, DESCRIPTOR(proc_time), __ALERT_WINDOW__))
  GROUP BY merchant_id, window_start, window_end
  HAVING COUNT(*) >= 20
     AND SUM(CASE WHEN match_status <> 'MATCHED' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) > 0.10;
END;
