# Architecture

How the pipeline works, end to end: the event-time model, the joins, the
sinks, the state, and the data model of every table.

---

## 1. The three legs

| leg | topic | producer | arrives | faults simulated |
|---|---|---|---|---|
| switch authorisation | `upi.switch` | PSP switch log | real time | duplicate RRN on retry |
| merchant callback | `upi.callbacks` | merchant webhook | seconds | missing, NACK, clock-skewed *before* the switch |
| bank settlement | `upi.settlements` | NPCI/bank file | minutes–hours | missing, late, amount drift |
| ground truth | `recon.ground_truth` | the generator | with the fault | — (the oracle) |

All four are Avro with a Confluent Schema Registry. The schema is deliberately
flat and boring:

```json
{ "rrn": "…", "merchant_id": "M0001", "amount_paise": 149900,
  "status": "SUCCESS", "event_ts": 1792526953560 }
```

`event_ts` is epoch **milliseconds** (a plain `long`). The Avro contract
therefore does not depend on Flink's logical-type mapping, and the event time
is reconstructible from any consumer.

Amounts are `BIGINT` **paise** everywhere on the wire. A single float rupee in
a reconciliation pipeline is how you end up explaining a ₹0.01 break to an
auditor. `DECIMAL(14,2)` appears only at the presentation boundary (the
sinks, the gold layer, the dashboard).

---

## 2. The event-time model

```
switch_ts  ──────────────►  decision (fast job)      ──► t0 + 4 min
   │                             │
   ├── callback_ts ≈ switch_ts   │  (± 2 min window, either direction)
   │                             ▼
   └── settle_ts  ──────────►  decision (repair job)  ──► settle_ts
```

* Each source declares its own watermark: `event_ts_ltz - <lag>`, with the
  switch/callback at 30 s and the settlement at 5 min (production units). The
  settlement watermark lags further because settlements arrive later — that is
  what makes a missing settlement detectable at all.
* The **callback window is symmetric** (± 2 min) so that a webhook with a
  skewed clock — arriving *before* the switch record — still matches.
* The **settlement window is one-sided** (`[switch_ts, switch_ts + 4 h]`): a
  settlement that predates the switch is not a settlement of that payment.
* The repair job's window is `(4 h, 24 h]` — exactly the complement of the
  fast job's, so the two never fight over a row.

The time attributes are computed in SQL, not in the Avro schema:

```sql
event_ts BIGINT,
event_ts_ltz AS TO_TIMESTAMP_LTZ(event_ts, 3),
WATERMARK FOR event_ts_ltz AS event_ts_ltz - INTERVAL '5' SECOND
```

---

## 3. The fast job (`flink/20_recon.sql`)

```
switch_txn ──┐
callback   ──┼─► interval joins (bounded state) ─► decision rows ─► dedup ─┐
settlement ──┘                                                              │
                                                                         ▼
                        ┌──────────────┬───────────────┬──────────┬──────────┤
                        ▼              ▼               ▼          ▼          ▼
                 pg_match_results pg_decision_audit  Iceberg  KPI window  alerts
```

**The joins.**

```sql
FROM switch_txn s
LEFT JOIN callback c
  ON s.rrn = c.rrn
 AND c.event_ts_ltz BETWEEN s.event_ts_ltz - __CB_WINDOW__
                        AND s.event_ts_ltz + __CB_WINDOW__
LEFT JOIN settlement st
  ON s.rrn = st.rrn
 AND st.event_ts_ltz BETWEEN s.event_ts_ltz
                        AND s.event_ts_ltz + __SETTLE_WINDOW__
WHERE s.status = 'SUCCESS'
```

`LEFT JOIN` on the settlement is what produces the exception: when the
settlement watermark passes `switch_ts + 4 h`, Flink emits the row with
`settle_amount IS NULL`, and the decision turns it into
`MISSING_SETTLEMENT`. No custom timers, no state store to operate.

**The dedup** (after the join, for the reason in
[decision D2](DECISIONS.md)):

```sql
ROW_NUMBER() OVER (PARTITION BY rrn ORDER BY switch_ts ASC) = 1
```

**The decision** (the settlement leg has precedence — it is the money leg):

```sql
CASE
  WHEN st_amt IS NULL                               THEN 'MISSING_SETTLEMENT'
  WHEN cb_amt IS NULL OR ack_status <> 'ACK'        THEN 'MISSING_CALLBACK'
  WHEN sw_amt <> st_amt OR sw_amt <> cb_amt         THEN 'AMOUNT_MISMATCH'
  ELSE 'MATCHED'
END
```

**The fan-out** is a single `EXECUTE STATEMENT SET`, so all five sinks see the
same records in one job and one checkpoint:

| sink | mode | key | purpose |
|---|---|---|---|
| `pg_match_results` | upsert | `rrn` | the current state, monotonic in decision time |
| `pg_decision_audit` | upsert | `(rrn, decision_ts)` | the history of decisions |
| `lake.recon.match_results` | append | — | the durable log, partitioned by decision day |
| `pg_merchant_minute_stats` | upsert | `(merchant_id, window_start)` | 1-minute health |
| `pg_alerts` | upsert | `(merchant_id, window_start)` | 2-minute merchant breaches |

---

## 4. The repair job (`flink/30_late_correction.sql`)

Identical in shape, different in three ways:

1. **Its own sources**, with their own Kafka consumer group ids
   (`recon-*-late`) — two jobs in one consumer group would split the
   partitions and the repair job would see nothing
   ([decision D13](DECISIONS.md)).
2. **A different window**: the settlement must arrive *after* the fast job's
   bound and *before* the repair bound.
3. **A different `decision_ts`**: the settlement time, not the switch time, so
   the correction gets its own key in the audit and wins in the serving table
   ([decisions D3, D4](DECISIONS.md)).

```sql
INNER JOIN settlement_late st
  ON s.rrn = st.rrn
 AND st.event_ts_ltz BETWEEN s.event_ts_ltz + __SETTLE_WINDOW__
                         AND s.event_ts_ltz + __LATE_WINDOW__
```

The `INNER JOIN` is the point: a settlement outside the window produces no row
at all, so the repair job can never invent a decision.

---

## 5. State, checkpointing and recovery

| property | value | why |
|---|---|---|
| state backend | RocksDB, incremental | the join state is large; the heap backend exhausted the heap and stalled the heartbeat |
| checkpoint interval | 30 s | fewer Iceberg commits, less back-pressure |
| checkpoint mode | exactly once | the sinks are transactional/upsert |
| retained checkpoints | `RETAIN_ON_CANCELLATION` | a resubmit can restore instead of replaying |
| source offsets | `earliest-offset` | a resubmit replays the topic and rebuilds the state; the deterministic audit key makes the replay idempotent |
| parallelism | 1 per job | one Kafka partition per topic, and the join state is cheaper in one shard than in many |

**Why a replay is safe.** The audit key `(rrn, decision_ts)` is derived from
the *event* time, so re-deciding a transaction produces the same key and the
same values. The serving table is monotonic in `decision_ts`, so a stale
replay cannot undo a correction. Replaying is therefore not just safe, it is
*useful*: it fills the gaps a crash left behind.

---

## 6. The data model

### `recon.match_results` — the serving table

| column | type | note |
|---|---|---|
| `rrn` | TEXT PK | the natural key |
| `merchant_id` | TEXT | |
| `match_status` | TEXT | one of the four decisions |
| `switch_amount`, `settle_amount`, `callback_amount` | NUMERIC(14,2) | rupees |
| `switch_ts`, `settle_ts`, `callback_ts` | TIMESTAMPTZ | event times |
| `settlement_lag_s` | INTEGER | NULL when there is no settlement |
| `decision_ts` | TIMESTAMPTZ | the event time the decision is anchored to |
| `decided_at` | TIMESTAMPTZ | when the pipeline wrote it |

Plus `trg_match_results_monotonic`, a `BEFORE UPDATE` trigger that rejects a
write with an older `decision_ts`.

### `recon.decision_audit` — the history

The same columns plus `audit_id`, with `UNIQUE (rrn, decision_ts)`. One row
per *decision*: a corrected RRN has two rows (`MISSING_SETTLEMENT` anchored at
the switch time, `MATCHED` anchored at the settlement time).

```sql
select rrn, match_status, decision_ts, decided_at
from recon.decision_audit
where rrn in (
  select rrn from recon.decision_audit group by rrn
  having bool_or(match_status = 'MISSING_SETTLEMENT')
     and bool_or(match_status = 'MATCHED')
)
order by rrn, decision_ts;
```

### `recon.merchant_minute_stats` / `recon.alerts`

The operational tables: one row per merchant per processing-time minute, with
the transaction counts and the unmatched count; the alert table is the subset
where the unmatched rate breached 10 % with at least 20 transactions in the
window.

### `recon.ground_truth` — the oracle

One row per injected fault: `rrn`, `fault`, `merchant_id`, `expected_status`,
`expected_initial`. For `LATE_SETTLE` the *initial* status is
`MISSING_SETTLEMENT` and the *final* status is `MATCHED` — the repair job has
to move it. For every other class the two are equal.

### `lake.recon.match_results` — the durable log

One row per decision, appended, partitioned by `decided_date`. Append-only on
purpose ([decision D6](DECISIONS.md)); the dbt staging model deduplicates it
to "latest decision per RRN".

---

## 7. The generator

`generator/model.py` is a dependency-free model of the fault semantics: it
decides which fault a transaction gets, builds the three legs with their
delays, and produces the ground-truth row. It has no Kafka, no database and
no Docker, so the whole fault matrix is unit-testable on a laptop.

`generator/txn_generator.py` is the producer: it maintains a heap of
scheduled legs, so a settlement that is due in 45 minutes is produced in 45
minutes (at the scaled rate) rather than immediately. It drains the whole
heap on shutdown — a leg that is due later still belongs to a transaction the
pipeline has already seen.

`generator/truth_sink.py` consumes the ground-truth topic and upserts it into
Postgres, deduplicating each batch by RRN (Postgres refuses an
`ON CONFLICT ... DO UPDATE` that touches the same key twice).

---

## 8. The gold layer (dbt)

DuckDB is the engine; the Iceberg snapshot is pinned through the REST catalog.

| model | purpose |
|---|---|
| `stg_match_results` | the Iceberg log, deduplicated to the latest decision per RRN |
| `stg_ground_truth` | the oracle from Postgres |
| `mart_daily_recon` | finance control totals per day and merchant |
| `mart_exceptions_queue` | the ops work queue: ageing bucket and a suggested action |
| `mart_detection_quality` | recall per fault class, initial and final, plus the late-correction count |
| `mart_detection_precision` | how many flagged exceptions were genuine injected faults |

The control-total identity the finance view is built on:

```
gross = reconciled + mismatch + outstanding + missing_callback
```

`mismatch_inr` is the **full switch value** of the mismatched transactions,
not the difference between the legs. Using the difference would make the
control total look balanced while the money is still unresolved;
`mismatch_delta_inr` is the ops investigation amount.
