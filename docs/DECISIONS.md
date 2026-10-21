# Design decisions

Every decision that shaped this pipeline, with the context, the choice, the
alternative that was rejected, and what the choice costs. The failure log
([FAILURE_LOG.md](FAILURE_LOG.md)) has the implementation detail; this file has
the reasoning.

---

## D1 — Two bounded jobs instead of one large state store

**Context.** A settlement that arrives late must eventually be reflected, but
the pipeline must also declare a missing settlement *quickly* (that is what
operations acts on) and the state must not grow with the settlement tail.

**Choice.** Two Flink jobs over the same topics:

| job | state window | what it does |
|---|---|---|
| `upi-recon-main` | 4 h (scaled: 4 min) | decides everything, declares `MISSING_SETTLEMENT` when the bound passes |
| `upi-recon-late-correction` | 24 h (scaled: 24 min) | joins only the settlements in `(4 h, 24 h]` and corrects the RRN |

**Rejected.** One job with a 24 h window: it would hold 6× the state and
delay every `MISSING_SETTLEMENT` by 20 hours — the exact opposite of what
operations needs. Or one job with an unbounded window: state grows forever.

**Cost.** The same RRN can be decided twice, so the serving table has to be
idempotent (D3, D4) and the two jobs need separate consumer groups (D13).

---

## D2 — The RRN dedup runs *after* the interval join

**Context.** A switch retry has the same RRN and the same event time. It must
not become a second exception and must not inflate the finance totals.

**Choice.** Join the raw (append-only) Kafka streams, then deduplicate the
*decision rows*:

```sql
ROW_NUMBER() OVER (PARTITION BY rrn ORDER BY switch_ts ASC) = 1
```

**Rejected.** The obvious `dedup → join`. It does not plan: a Flink interval
join can only consume an append-only input, and `ROW_NUMBER()` dedup is a
retracting operator:

```
StreamPhysicalIntervalJoin doesn't support consuming update and delete changes
which is produced by node Deduplicate(keep=[FirstRow], key=[rrn], order=[ROWTIME])
```

**Cost.** The join sees duplicates, so it does slightly more work. It also
means the *dedup* is a retracting stream in principle; in practice the retry
is a byte-identical copy, so the operator never has to retract a row it
already emitted and the sinks stay insert-only.

---

## D3 — The audit is keyed by event time, not wall clock

**Context.** The audit must be a faithful history: "this RRN was declared
missing, and later corrected". It must also survive a replay.

**Choice.** The audit's natural key is `(rrn, decision_ts)`, where
`decision_ts` is the **event time the decision is anchored to**:

| job | `decision_ts` | meaning |
|---|---|---|
| fast | `switch_ts` | the decision is taken when the switch is seen |
| repair | `settle_ts` | the correction is taken when the late settlement arrives |

`decided_at` (the wall clock) is still stored, for operational questions.

**Rejected.** `(rrn, decided_at)` with `decided_at = CURRENT_TIMESTAMP`. It
looks right, but every replay re-decides every transaction and invents a new
history row — the audit inflated 2.2× per restart and the "first decision was
MISSING_SETTLEMENT" fact became unverifiable.

**Cost.** One more column and a slightly more subtle key. The benefit is that
a replay is *idempotent* and the history is *ordered by the events that caused
it*.

---

## D4 — The serving table is monotonic in decision time

**Context.** The fast job and the repair job write the same RRN. A replay of
the fast job can arrive after the repair job's correction.

**Choice.** A `BEFORE UPDATE` trigger rejects a write whose `decision_ts` is
older than the stored one:

```sql
IF NEW.decision_ts < OLD.decision_ts THEN RETURN OLD; END IF;
```

**Rejected.** Last-write-wins. It produced 23 RRNs that flipped from
`MATCHED` back to `MISSING_SETTLEMENT` — the pipeline contradicting itself.

**Cost.** The correctness now depends on a database trigger rather than on the
application. It is five lines of PL/pgSQL and it is the only piece of
"business logic" outside the Flink job; the alternative (a version column
compared in the sink) is not expressible with the JDBC connector.

---

## D5 — Postgres serves the current state, Iceberg keeps the log

**Context.** Two consumers: the dashboard and the ops queries need "the
current decision per RRN" with a point lookup, and the data platform needs a
durable, replayable history.

**Choice.**

* `recon.match_results` (Postgres) — one row per RRN, upserted, monotonic.
  Read by the dashboard, the ops queries and the integration tests.
* `lake.recon.match_results` (Iceberg) — one row per *decision*, append-only,
  partitioned by decision day. Read by the dbt gold layer.

**Rejected.** Iceberg as the serving store: a key-value lookup on a table
that is written every 30 seconds is not a serving path, and the dbt layer
would have to do the "latest decision per RRN" reduction on every read.
Postgres as the only store: no cheap replay, no columnar analytics, no
history.

**Cost.** Two stores to keep in step. The audit↔serving pair is not atomic
(known limitation), and the Iceberg table grows with the decision volume.

---

## D6 — The Iceberg table is append-only (equality deletes off)

**Context.** The natural design for a "hot store" in Iceberg is an upsert
table with equality deletes.

**Choice.** `write.upsert.enabled = false`. The table is a decision log; the
dbt staging model applies the "latest decision per RRN" rule.

**Rejected.** Upsert with equality deletes. DuckDB's `iceberg_scan`
materialises the equality-delete index per data file, and on this table that
needed **more than 2 GB of RAM to read 9 MB of Parquet** — more than fits next
to the streaming stack on an 8 GB laptop. Without deletes the same scan costs
0.3 s and 110 MB.

**Cost.** The lake holds one row per decision instead of one per RRN, so it
grows faster and the staging model has to deduplicate. In exchange the gold
layer reads the lake in seconds on a laptop.

---

## D7 — Processing-time windows for the KPIs and alerts

**Context.** A `MISSING_SETTLEMENT` is emitted when the watermark passes the
settlement bound — that is, several minutes *after* the switch happened, in
event time.

**Choice.** `TUMBLE(..., DESCRIPTOR(proc_time), INTERVAL '1' MINUTE)` — the
KPI bucket and the alert window are aligned to the wall clock, not to the
event clock.

**Rejected.** Event-time windows. The exception would be attributed to the
minute in which the payment happened, hours before anyone could act on it.
Operations care *when the decision was made*.

**Cost.** The KPI is not reproducible from a replay of the same data (the
bucket depends on when the row was processed). For a demo whose point is
"what does operations see, and when", that is the right trade.

---

## D8 — The oracle lives outside the Flink path

**Context.** Recall and precision are only meaningful against an independent
truth.

**Choice.** The generator publishes the injected faults to a separate Kafka
topic (`recon.ground_truth`); a separate sink (`truth_sink.py`) upserts them
into `recon.ground_truth`. Flink never reads it.

**Rejected.** Writing the truth from inside the Flink job (e.g. a
`Values` table or a side-output). Then a bug in the job could corrupt the
oracle it is being scored against.

**Cost.** One more container and one more topic, and the oracle can lag the
generator (it tracks it within ~10 records).

---

## D9 — `TIME_SCALE` compresses the delays *and* the timestamps

**Context.** The interesting behaviour (a settlement 4 hours late) is not
demoable in 4 hours.

**Choice.** One knob scales the generator's delays and the event timestamps
together, and `flink/render_sql.sh` substitutes the scaled values into the
SQL at submit time. The SQL files stay in production units.

**Rejected.** Scaling only the delays. The generator would then emit events
whose timestamps and delays disagree, and the event-time joins would produce
nonsense (a settlement "4 hours" later but with a timestamp 4 seconds later).

**Cost.** The rendered SQL differs from the checked-in SQL, so a reader has to
know about `render_sql.sh`. The scale also has to stay consistent across the
watermarks, the bounds and the callback window, which is why the render script
computes them all in one place.

---

## D10 — The resource envelope is sized for an 8 GB laptop

**Context.** The stack has to be runnable on one machine, and the interval-join
state is `rate × window`.

**Choice.**

| knob | value | why |
|---|---|---|
| demo rate | 100 TPS | 500 TPS needs more state than 8 GB allows |
| TaskManager | 2 816 MB, RocksDB, 4 slots | the state lives on disk; the slots fit both jobs |
| heartbeat timeout | 120 s | a laptop can miss a 50 s heartbeat while restoring |
| Kafka | 1 GB heap | a starved broker freezes the watermark, and the join state then grows without bound |
| dbt | 1 GB, DuckDB 256 MB, 2 threads | the gold build runs next to the streaming stack |

**Rejected.** Sizing for production (multiple TaskManagers, RocksDB on a
tuned cluster, 1000+ TPS). The pipeline would be right but not runnable, and
an unrunnable demo is not a demo.

**Cost.** The demo rate is two orders of magnitude below a real PSP. The
architecture is unchanged by that, but any claim about throughput has to be
read as "sustains whatever the generator produces" rather than as an absolute
number.

---

## D11 — RRNs are allocated, not drawn

**Context.** The RRN is the natural key of the entire pipeline.

**Choice.** A random 4-digit prefix plus a monotonic counter, unique per
process, still 12 digits, O(1) memory.

**Rejected.** `rng.randint(10**11, 10**12 - 1)`. The birthday probability of
a collision is ~4 % at 270 k transactions, and a collision merges two unrelated
payments into one decision — we saw exactly one such case, a `MATCHED` turning
into `AMOUNT_MISMATCH`.

**Cost.** The RRNs of two runs can collide (a different prefix each time) —
a 3 × 10⁻⁷ chance per run, acceptable for a demo whose data is wiped
regularly. A production generator would use a database sequence or a UUID.

---

## D12 — The JDBC sinks use `TIMESTAMP(3)`, Iceberg uses `TIMESTAMP_LTZ(3)`

**Context.** The events are epoch milliseconds on the wire; the decision
carries several timestamps.

**Choice.** The Avro contract stays a plain `long` (epoch ms) and Flink
derives `event_ts_ltz = TO_TIMESTAMP_LTZ(event_ts, 3)`. The Postgres sinks
declare `TIMESTAMP(3)` (the JDBC connector has no `TIMESTAMP_LTZ` converter);
the Iceberg sink keeps `TIMESTAMP_LTZ(3)`. The session time zone is pinned to
UTC.

**Rejected.** `TIMESTAMP_LTZ` everywhere — the connector simply cannot write
it. Or `TIMESTAMP` everywhere — then the Iceberg analytics lose the zone and
the DuckDB/dbt layer has to assume one.

**Cost.** Two representations of the same instant in two stores, and an
explicit cast in every insert. The pinned UTC zone keeps them comparable.

---

## D13 — Each job gets its own Kafka consumer groups

**Context.** Both jobs read the same three topics.

**Choice.** `recon-switch` / `recon-settle` / `recon-callback` for the fast
job, `recon-*-late` for the repair job.

**Rejected.** Sharing the group ids. Two Flink jobs in one consumer group are
two members of that group; the coordinator hands each partition to exactly one
member. The fast job won all three partitions and the repair job received
*nothing* — silently, with no error anywhere.

**Cost.** The topics are read twice (2× the broker traffic). A different group
id per job is the price of two independent readers.

---

## D14 — The SQL is submitted through the SQL Gateway, not an embedded client

**Context.** The SQL has to be executed against the running cluster.

**Choice.** `flink-sql-submit` runs `sql-client.sh gateway --endpoint
flink-sql-gateway:8083 -f <rendered file>` in a one-shot container. The
submit container's `FLINK_PROPERTIES` carry `rest.address: flink-jobmanager`,
because the client sends its own configuration to the gateway and the gateway
submits the job with it.

**Rejected.** `sql-client.sh -f ...` inside the JobManager container. In
embedded mode the client starts its own MiniCluster instead of talking to the
running cluster, and the job would never appear in the Flink UI.

**Cost.** One more container and the need to keep the client's configuration
and the gateway's in sync.

---

## D15 — The truth sink collapses duplicate keys in a batch

**Context.** Postgres refuses an `ON CONFLICT DO UPDATE` that touches the same
key twice in one statement.

**Choice.** Deduplicate the batch by RRN before the upsert, and fall back to
row-by-row inserts if the batch still fails.

**Rejected.** Letting it fail. The sink crash-looped, never committed its
offsets, and the oracle froze — which silently invalidates every recall
number.

**Cost.** A few lines of Python and a per-batch dictionary.

---

## D16 — The demo runs at 100 TPS and the ground truth is the oracle

Covered by D10 and D8; listed here because the combination is what makes the
demo self-verifying: the pipeline and the oracle are produced by the same
generator but travel completely different paths, so the recall numbers are a
real measurement rather than a tautology.
