# Study guide: UPI three-way reconciliation

A document that explains the whole project from zero, in plain language, and
then lists the questions that can be asked about it — with answers.

**Read it in one of three ways**

| If you have… | Read |
|---|---|
| 30 seconds | §0 |
| 10 minutes | §0 → §2 → §5 → §11 A/B (skim) |
| A full afternoon | everything, in order, and then the other six documents |

**What this project is:** a streaming pipeline that decides, for every UPI
payment, whether the money actually arrived — within minutes, not the next
day — and keeps a replayable audit trail of every decision it made.

**What it is not:** a payment gateway, a ledger, a settlement engine, or a
product. It is a *reference implementation* of one hard problem, built so that
the design can be read, argued with, and copied.

---

## 0. The one-paragraph version

Three systems each record the same payment in their own way and on their own
clock: the **switch** (the payment network authorises it), the **merchant
callback** (the merchant's server is told), and the **bank settlement** (the
money lands, a day later, net of fees). Today's reconciliation compares
those three records overnight and produces a spreadsheet of mismatches a day
too late, when the customer has already been debited. This project instead
joins the three legs **in event time** as they stream, and emits one of four
verdicts per payment — `MATCHED`, `AMOUNT_MISMATCH`, `MISSING_CALLBACK`,
`MISSING_SETTLEMENT` — usually within a minute, with every decision recorded
in an append-only audit trail, plus a live exception queue for ops and daily
control totals for finance.

---

## 1. The problem, in plain words

### 1.1 What a UPI collection is

A customer pays a merchant. Three different organisations see that payment:

1. **The payment network (switch)** authorises it and routes it to the
   merchant's bank. It knows the payment succeeded. It logs it.
2. **The merchant's server** receives a webhook ("payment successful") and
   marks the order paid. It knows the payment succeeded. It logs it.
3. **The bank (via NPCI)** sends a settlement file — the money, grouped and
   net of fees — usually a day later, and the merchant's bank credits it.

Reconciliation is the job of proving these three stories are the same story,
and of finding out quickly when they are not.

### 1.2 The three legs

| Leg | Where it comes from | How fast | What it can get wrong |
|---|---|---|---|
| Switch / auth | the PSP's switch log | real time | a retried request appears twice |
| Merchant callback | the merchant's webhook | seconds | never arrives; arrives as NACK; arrives with a skewed clock |
| Bank settlement | NPCI / bank file | minutes to hours | never arrives; arrives late; amount differs from the order |

### 1.3 Why it is genuinely hard

Five things make this harder than "join three tables on an id":

1. **Three clocks.** The switch says 10:00:00, the callback says 10:00:04
   (or 09:59:50, because that merchant's server clock drifts), the settlement
   file says "the batch for the 14th". A join on *arrival* order is wrong; you
   must join on *event* time.
2. **Three identifiers.** The switch knows the RRN (the network's reference),
   the merchant knows the order id, the bank knows its own reference. They are
   different keys for the same payment, and mapping them is real work.
3. **The settlement is net of fees.** The customer paid ₹1,499, the
   settlement says ₹1,470. The ₹29 difference is MDR, not a break. If fee
   lines are not their own rows, every reconciliation looks broken.
4. **Money in transit is normal.** The switch says "paid" on Monday; the bank
   credits Wednesday. "No settlement yet" is not an exception on Monday — it
   is the normal state of the world. A system that cries wolf on every
   payment is a system that gets ignored.
5. **Duplicates and corrections are indistinguishable from the real thing.** A
   retried switch request produces two byte-identical records. A late
   settlement produces a *second* opinion about a payment that was already
   decided.

### 1.4 What the batch alternative costs

| | T+1 batch | This pipeline |
|---|---|---|
| "Is this payment OK?" | next day, in a spreadsheet | ~30 seconds, per payment |
| Customer already debited | yes, and nobody noticed | yes, but the exception exists in minutes |
| Shortfall discovered | at close, 4–6 weeks of payments later | same day |
| Re-running after a fix | re-process a file, re-diff | replay the stream; the audit key makes it idempotent |
| Audit trail | the spreadsheet that was edited | every decision, append-only, keyed by event time |

### 1.5 An analogy that holds up

Think of a courier delivering one parcel. The **switch** is the order being
accepted by the courier's app. The **callback** is the recipient's phone
notifying "delivered". The **settlement** is the courier's own accounting
showing the cash in the bank a day later, minus the platform's cut.

Reconciling means proving the app, the phone, and the bank account all refer
to the same parcel. The parcel is the RRN. The three timestamps are the three
clocks. The courier's fee is the MDR. And "it's still in the van" is not a
failure — it is a state with an expected delivery date.

---

## 2. What the system does

### 2.1 The 30-second version

A simulator produces three streams of events. Kafka carries them. A Flink SQL
job joins the three legs **within a 4-hour event-time window** and writes a
verdict per payment into Postgres (current state) and Iceberg (permanent log).
A second Flink job keeps a **24-hour window** and only looks at settlements
that arrived after the first job's window closed, so a payment wrongly called
"missing" gets corrected. dbt turns the log into finance control totals, an
ops exception queue and a detection-quality report. A generator publishes
the *fault it injected* on a side topic, so every run is scored against ground
truth: currently 100 % recall on all six fault classes, 0 false positives, and
control totals balanced to ₹0.00.

### 2.2 The component map

| Piece | In plain English | Why it exists | Where |
|---|---|---|---|
| **Generator** (`generator/`) | A fake payment network that behaves like a real one, including the mistakes | Without realistic faults there is nothing to reconcile against | `generator/model.py` (pure logic), `txn_generator.py` (producer), `truth_sink.py` (oracle sink) |
| **Kafka** | The bus the events travel on | Decouples the producer from the consumer; lets you replay | 4 topics: `upi.switch`, `upi.callbacks`, `upi.settlements`, `recon.ground_truth` |
| **Flink SQL** (fast job) | The decision engine: joins the three legs, decides, writes | Stream processing with event-time joins and bounded state | `flink/20_recon.sql` |
| **Flink SQL** (repair job) | The late-corrector: re-decides payments whose settlement arrived late | A 4-hour window alone gets late settlements wrong | `flink/30_late_correction.sql` |
| **Postgres** | The "what is true right now" store, plus the audit trail and the ops tables | Fast, transactional, and easy for a dashboard to read | `postgres/init/01_recon.sql` |
| **Iceberg (on MinIO/S3)** | The permanent log: every decision ever made, append-only | Replayable history; the audit survives a bad query | `flink/00_catalogs.sql` |
| **dbt on DuckDB** | The reporting layer: daily control totals, exception queue, quality report | Finance and ops should not write SQL against the raw log | `dbt/models/` |
| **Streamlit dashboard** | The screen: live decisions, exception queue, alerts, quality | Ops needs a queue, not a query language | `dashboard/app.py` |
| **GitHub Actions** | Runs the entire stack on a runner every 30 minutes, validates it, publishes the metrics | The project proves itself continuously, with no server to babysit | `.github/workflows/simulation.yml` |
| **Vercel** | Serves the docs and a public dashboard of the last run | Where a stranger can see the numbers | `site/`, `scripts/build_site.mjs` |

Everything runs in **11 Docker containers, ~7 GB of memory**, on a laptop.

### 2.3 Where it runs

| Deployment | How |
|---|---|
| Laptop | `make up` — the whole stack, plus the Flink UI on :8087 and the dashboard on :8503 |
| CI, no server | the `simulation` workflow runs the whole stack on a GitHub runner every 30 min, validates it and publishes the metrics to the repo |
| Public | Vercel serves the docs and `/dashboard` (the live simulation view) |
| Production-shaped | the same `docker compose` on a VPS, with managed Postgres and the Iceberg warehouse on S3/R2 |

Flink itself cannot run on Cloudflare Workers or Vercel — it is a long-lived
JVM with stateful operators. Everything around it can.

---

## 3. One payment, end to end

### 3.1 The happy path

A customer pays ₹1,499.00 to merchant `M0001` at 10:00:00.

| t | what happens | where |
|---|---|---|
| 10:00:00 | switch writes `rrn=R1, amount=149900 paise, status=SUCCESS` | `upi.switch` |
| 10:00:04 | merchant callback writes `rrn=R1, amount=149900, ack=ACK` | `upi.callbacks` |
| 10:01:00 | the fast job's join completes: all three legs present, all three amounts equal | Flink fast job |
| 10:01:00 | writes `MATCHED` to `recon.match_results`, appends the decision to the audit, appends to the Iceberg log | Postgres + Iceberg |
| next day | the settlement arrives, net of ₹29 MDR → amount differs | `upi.settlements` |
| next day | the fast job is long gone (its window closed); the **repair job** sees the settlement, re-decides `R1` as `AMOUNT_MISMATCH` | Flink repair job |
| next day | the serving table now says `AMOUNT_MISMATCH` (the newer decision wins), and the audit holds **two** rows for `R1` | Postgres |

*Note the last step: a settlement that arrives after the 4-hour window is
invisible to the fast job — that is the whole point of bounded state. The
repair job, with a 24-hour window, picks it up and corrects the verdict.*

### 3.2 The late-settlement path (the interesting one)

Same payment, but the settlement arrives 9 hours late.

1. The fast job's 4-hour window closes with no settlement → it emits
   `MISSING_SETTLEMENT` (anchored at the switch time).
2. Ops sees an exception. *This is a false positive on every ordinary
   payment* — the money simply has not arrived yet. This is the project's
   known weakness and gap **G1**.
3. The settlement arrives at hour 9 → the repair job's window `(4 h, 24 h]`
   matches it, and emits `MATCHED` (anchored at the *settlement* time).
4. Because the audit is keyed on `(rrn, decision_ts)` and the serving table is
   monotonic in `decision_ts`, the newer decision wins and the older one stays
   as history. No delete, no update-in-place, no lost history.
5. The exception disappears from the queue — because the queue reads the
   *current* state, not the log.

### 3.3 The duplicate path

The switch is retried and the same `rrn` appears twice. The fast job's dedup
(`ROW_NUMBER() OVER (PARTITION BY rrn ORDER BY switch_ts ASC) = 1`) keeps one
row per RRN, so a retry cannot change the verdict or create a second
exception. Dedup runs **after** the join, deliberately — see ADR **D2** for
why (a Flink interval join cannot consume a retracting dedup as its input).

### 3.4 The timeline

```
event time
   │
10:00 ├─ switch ─────────┐
10:00 ├─ callback ───────┤  fast job window [switch_ts, switch_ts + 4h]
   │   │                 │
   │   │            decision ①  MISSING_SETTLEMENT  (no settlement yet)
   │   │
19:00 ├─ settlement ────────────────────�┐
   │                                    │  repair job window (4h, 24h]
   │                               decision ②  MATCHED  (anchored here)
   │
   └── serving table shows the latest decision; the audit keeps both.
```

---

## 4. The six ideas you must understand

Everything in this project is a consequence of these six. If you understand
them, you can derive the rest.

### 4.1 Event time vs processing time

* **Processing time** is when *our system* saw the event. **Event time** is
  when the event *happened*, according to the event's own timestamp.
* Reconciliation must use event time. A settlement that arrived at 03:00
  about a payment from 14:00 the previous day is *late data*, not *current
  data*, and the join has to place it next to the payment it belongs to.
* In Flink, this is done with `event_ts_ltz` plus a `WATERMARK FOR` clause.
  The watermark is Flink's promise: "I will not see any more events with a
  timestamp older than this." The switch and callback lag 30 s, the
  settlement 5 min — the settlement lags more because settlements *are*
  later, and that lag is exactly what makes a missing settlement detectable.

### 4.2 Watermarks

A watermark is how a stream processor says "the future has arrived up to
here". Everything downstream (window firing, join expiry) is driven by it.

* Without a watermark, a join never knows it may be finished, so state grows
  forever and nothing is emitted.
* With a *too aggressive* watermark (say, zero lag), a slightly-late
  callback is dropped and you get a false `MISSING_CALLBACK`.
* With a *too conservative* watermark (say, 1 hour lag), you wait an hour
  before you can say "missing" — you trade latency for correctness.
* The project uses a 5-minute settlement lag and a 4-hour join bound, so
  "missing" is only ever declared once the watermark has passed
  `switch_ts + 4 h`. No timers, no custom state.

### 4.3 Bounded interval join, and the memory equation

A **join** pairs the switch record with a callback and a settlement. A
**temporal (interval) join** pairs them only if their timestamps are close
enough — here: callback within ±2 minutes of the switch, settlement within
`[switch_ts, switch_ts + 4 h]`.

To do that, the job must *remember* every switch record for the length of the
window. That memory is:

```
state size  ≈  rate  ×  window  ×  (bytes per record)
             100/s    ×  4 h      ≈  1.44 M switch records
```

and the repair job needs the same for 24 h. Bounded state is not a stylistic
choice — it is what makes the job survivable on a laptop, and it is what
*defines* when a payment may be declared missing.

### 4.4 Two windows: the central trade-off

| | Fast job | Repair job |
|---|---|---|
| Window | 4 h of state | 24 h of state |
| State cost | 1× | 6× |
| Verdict latency | minutes | hours |
| Sees | settlements in `[switch_ts, switch_ts + 4 h]` | settlements in `(4 h, 24 h]` |
| Join | `LEFT JOIN` (absence is meaningful) | `INNER JOIN` (absence is meaningless) |

The two windows are **disjoint**, so the jobs never fight over a row. The
fast job is allowed to be wrong about late settlements because the repair job
will correct it; the repair job can never invent a decision because it only
ever joins settlements that exist. This is the single most important design
idea in the project (ADR **D1**).

### 4.5 Idempotency: the same decision twice is harmless

Two mechanisms, working together:

* **The audit key is `(rrn, decision_ts)`, and `decision_ts` is the *event*
  time of the decision, not the wall clock.** Re-deciding a transaction
  produces the same key and the same values, so a replay upserts the same
  row instead of appending a duplicate. A replay is a no-op — which means
  replaying is not merely safe, it is *useful*: it heals the gaps a crash
  left behind (ADR **D3**).
* **The serving table is monotonic in `decision_ts`**, enforced by a
  `BEFORE UPDATE` trigger in Postgres. A stale write (an old decision
  arriving late) is *rejected by the database*, not by application logic
  (ADR **D4**). Corrections therefore always win, and a correction can never
  be undone by a stale replay.

### 4.6 The oracle: how do we know it is right?

The generator knows which fault it injected. It publishes that knowledge on a
**separate topic** (`recon.ground_truth`), which a **separate process** writes
to Postgres. The oracle is deliberately *outside* the Flink path: if the
reconciler is wrong, the oracle must still be right (ADR **D8**).

Then dbt compares the two, per fault class, and reports **initial recall**
(what the fast job should have said first), **final recall** (what the
verdict should be after the repair job has run) and **precision** (how many
flagged exceptions were genuine injected faults). A CI job fails the build if
recall moves.

This is the part no commercial product in this space ships: not "here is our
dashboard", but "here is the evidence that the dashboard is right, and here
is the CI job that fails if it stops being right".

---

## 5. The decision table

The precedence is fixed, and the **settlement leg wins** because it is the
money leg: if the money did not arrive, nothing else matters.

| Settlement | Callback | Amounts | Verdict | What ops should do |
|---|---|---|---|---|
| missing | any | — | `MISSING_SETTLEMENT` | wait for the next cycle; after 24 h raise a UDIR dispute |
| present | missing or NACK | — | `MISSING_CALLBACK` | re-fire the webhook ×3, then refund |
| present | ACK | any leg differs | `AMOUNT_MISMATCH` | check the MDR/fee deduction, else chargeback with `bank_ref` |
| present | ACK | all equal | `MATCHED` | — |

In SQL it is a single `CASE` expression (see `flink/20_recon.sql`), and the
same table is mirrored in Python in `tests/test_decision_logic.py` so the
rules are unit-testable without Docker.

**Two subtleties**

* `MISSING_SETTLEMENT` is only reachable once the watermark passes
  `switch_ts + 4 h` — before that the row simply has not been emitted.
* `AMOUNT_MISMATCH` compares the **switch amount** with the other two legs.
  In the finance view, `mismatch_inr` is the *full switch value* of the
  mismatched transactions, not the difference between the legs: using the
  difference would make the control total look balanced while the money is
  still unresolved.

---

## 6. The fault matrix and the evidence

### 6.1 The six injected faults

| Injected fault | What the pipeline sees | First verdict | Final verdict |
|---|---|---|---|
| `DUP_SWITCH` | the same RRN twice in the switch topic | `MATCHED` | `MATCHED` |
| `AMOUNT_DRIFT` | the settlement amount differs | `AMOUNT_MISMATCH` | `AMOUNT_MISMATCH` |
| `MISSING_SETTLE` | no settlement row at all | `MISSING_SETTLEMENT` | `MISSING_SETTLEMENT` |
| `LATE_SETTLE` | the settlement arrives after the 4 h bound | `MISSING_SETTLEMENT` | `MATCHED` (repaired) |
| `MISSING_CALLBACK` | no merchant webhook | `MISSING_CALLBACK` | `MISSING_CALLBACK` |
| `OUT_OF_ORDER` | the callback timestamp precedes the switch | `MATCHED` | `MATCHED` |

Every 500th transaction gets a fault in round-robin order (so every class
appears even in a short run), and 20 % of the traffic with 35 % of the faults
is concentrated on merchant `M0001` — so the merchant-level alert (> 10 %
unmatched in a 2-minute window) actually fires instead of being averaged away.

### 6.2 What the latest published run measured

From the run published by the CI job on 2026-10-21 (25-minute-aged cohort):

| Metric | Value |
|---|---|
| Decisions emitted | 183,205 |
| `MATCHED` / `AMOUNT_MISMATCH` / `MISSING_CALLBACK` / `MISSING_SETTLEMENT` | 174,052 / 2,962 / 2,893 / 3,298 |
| Match rate | 95.0 % |
| Audit rows / distinct RRNs | 185,101 / 183,248 (the difference is late corrections) |
| Late corrections applied | 1,853 |
| Regressions | 0 |
| False positives | 0 |
| Final recall, all six fault classes | 100 % |
| Initial recall for `LATE_SETTLE` | 0 % *by design* — the fast job cannot see past its window |
| Decision latency | avg 37 s, p50 27 s, p95 54 s |
| Control total difference | ₹0.00 (gross 400,292,007.96 = reconciled + mismatch + outstanding + missing callback) |

### 6.3 The control-total identity

Finance needs the books to add up. The identity the pipeline maintains, per
day and per merchant:

```
gross = reconciled + mismatch + outstanding + missing_callback
```

`outstanding` is the money that is authorized but not yet settled — the
in-transit bucket. A run whose difference is not ₹0.00 is a run where a
rupee is unaccounted for, and the CI job treats that as a failure.

### 6.4 The test suite

```bash
make test              # 33 unit tests, no Docker needed
make integration-test  # 8 tests against the running stack
make dbt               # 23 dbt nodes (4 models + 4 marts + 15 tests)
```

* **Unit tests** pin the fault semantics and mirror the SQL `CASE` in Python,
  so the decision table is testable on a laptop with no Kafka, no database
  and no Docker.
* **Integration tests** assert recall ≥ 95 % per fault class, that a late
  settlement is first `MISSING_SETTLEMENT` and later `MATCHED`, that a
  duplicate never becomes an exception, and that no row contradicts its own
  status. They also *age* their samples: a decision cannot exist before the
  settlement bound has passed, so the tests only count samples old enough.
* **dbt tests** assert uniqueness, the match-rate range, the amount identity
  and the control-total balance.

---

## 7. The data model, in plain words

### 7.1 Which store holds what

| Store | One row is… | Written by | Why it exists |
|---|---|---|---|
| `recon.match_results` (Postgres) | one payment, **current** state | both Flink jobs, upsert | "what is true right now" for the dashboard and the exception queue |
| `recon.decision_audit` (Postgres) | one **decision** | both Flink jobs, upsert on `(rrn, decision_ts)` | the history: what we decided, and when *in event time* |
| `recon.merchant_minute_stats` | one merchant × one minute | fast job | operational health |
| `recon.alerts` | one merchant × one minute (breaches only) | fast job | "merchant M0001 is failing" |
| `recon.ground_truth` | one **injected fault** | the truth sink | the oracle |
| `lake.recon.match_results` (Iceberg) | one **decision**, forever, append-only | both Flink jobs | the durable, replayable log; the audit survives a bad query |

### 7.2 The two tables people confuse

`match_results` and `decision_audit` look almost identical. The difference is
the whole design:

* `match_results` is **state**: one row per RRN, always the latest decision.
  It is upserted, and a stale write is rejected by a database trigger.
* `decision_audit` is **history**: many rows per RRN, one per decision, and
  never deleted. A corrected payment has two rows — `MISSING_SETTLEMENT`
  anchored at the switch time, `MATCHED` anchored at the settlement time.

Ask the audit table "what did we decide about R1, and when did we decide it?"
and you get the full story, including the fact that we were wrong first and
righted it.

### 7.3 The Iceberg table is append-only, on purpose

Equality deletes (Iceberg's row-level update) are switched **off**
(ADR **D6**): DuckDB's equality-delete index needed more than 2 GB, and the
"latest decision per RRN" rule is a two-line `ROW_NUMBER()` in the dbt
staging model instead. The log stays append-only and cheap; the deduplication
is pushed to the reader. The cost: the log grows with the decision volume and
needs a maintenance job (`scripts/iceberg_maintenance.py`).

### 7.4 Money is never a float

Amounts are **integer paise** end to end on the wire and in the streaming
layer. `DECIMAL(14,2)` appears only at the presentation boundary (the sinks,
the gold layer, the dashboard). A single float rupee in a reconciliation
pipeline is how you end up explaining a ₹0.01 break to an auditor.

---

## 8. Running it

### 8.1 The commands

```bash
make up          # build + start everything, submit the two Flink jobs
make status      # jobs, decision counts, late corrections
make dbt         # build the gold layer against the pinned Iceberg snapshot
make test        # unit tests, no Docker required
make integration-test
make verify      # both
make submit      # cancel and resubmit both Flink jobs
make logs SERVICE=flink-taskmanager
make down        # stop, keep the data
make clean       # stop and delete the volumes
```

Then: dashboard on :8503, Flink UI on :8087, SQL Gateway on :8086, Kafka on
:19093, MinIO on :9000, Postgres on :5432.

### 8.2 Time scaling: how a 4-hour SLA becomes 4 minutes

Waiting four hours for a settlement is not a demo. `TIME_SCALE` compresses
**both the delays and the event timestamps**, so the event-time joins and the
generator still agree on ordering.

| Production concept | `TIME_SCALE=60` (default) |
|---|---|
| Settlement bound (4 h) | 240 s |
| Late-settlement repair bound (24 h) | 1,440 s |
| Callback match window (±2 min) | ±2 s |
| Switch/callback watermark (30 s) | 1 s |
| Settlement watermark (5 min) | 5 s |
| Source idle timeout (30 s) | 1 s |

The SQL files stay in production units; `flink/render_sql.sh` substitutes the
scaled values at submit time. `TIME_SCALE=1` gives production timing.

### 8.3 When it misbehaves

| Symptom | Likely cause | First move |
|---|---|---|
| No decisions at all | the Flink job is not running, or the generator is not producing | `make status`; then the TaskManager log |
| Decisions but all `MATCHED` | the fault injection is off (`FAULT_RATE=0`) or the generator is not running | check the generator log |
| The fast job is "busy" and the heartbeat stalls | the join state outgrew the heap | that is what RocksDB + incremental checkpoints are for (ADR D10); check the TaskManager memory |
| A late settlement never corrects | the repair job is not running, or its consumer group is shared with the fast job | each job needs its own consumer group (ADR D13) |
| The dashboard is empty | dbt has not run against the current Iceberg snapshot | `make dbt` |
| Recall dropped in CI | a real regression, not a flake — the run publishes the per-class recall so you can see which class moved | read the published `runs/latest.json` |

### 8.4 The CI simulation

Every 30 minutes, GitHub Actions starts the **entire stack** on a runner:
generator → Kafka → both Flink jobs → Postgres → dbt → the test suites. It
then publishes `runs/latest.json` and `runs/history.json` to the repo, Vercel
rebuilds, and the public dashboard updates. The project has no server to
babysit, and every published number is reproducible from a commit.

---

## 9. The design decisions, in plain words

Sixteen ADRs live in [DECISIONS.md](DECISIONS.md). The short list:

| # | Decision | One-line reason |
|---|---|---|
| D1 | Two bounded jobs, not one big state store | 6× the state for 6× the completeness; a single job would have to hold 24 h |
| D2 | Dedup **after** the interval join | a Flink interval join cannot consume a retracting dedup as input |
| D3 | The audit is keyed by **event** time, not wall clock | a replay then produces the same key → idempotent, and a replay heals gaps |
| D4 | The serving table is **monotonic** in decision time | a stale write must never undo a correction — enforced by a DB trigger |
| D5 | Postgres serves the current state; Iceberg keeps the log | hot reads want a database; durable history wants an open table format |
| D6 | The Iceberg table is append-only | equality deletes needed > 2 GB in DuckDB; dedup belongs in the reader |
| D7 | KPIs and alerts use **processing-time** windows | operational health is about "now", not about event time |
| D8 | The oracle lives **outside** the Flink path | if the reconciler is wrong, the oracle must still be right |
| D9 | `TIME_SCALE` compresses delays *and* timestamps | otherwise the event-time joins and the generator disagree on ordering |
| D10 | The resource envelope is sized for an 8 GB laptop | incremental RocksDB checkpoints, parallelism 1, bounded state |
| D11 | RRNs are **allocated**, not drawn | collisions would silently corrupt the join and the metrics |
| D12 | JDBC sinks use `TIMESTAMP(3)`, Iceberg `TIMESTAMP_LTZ(3)` | millisecond precision on both sides of the boundary |
| D13 | Each job has its **own** consumer groups | one shared group would split the partitions and the repair job would see nothing |
| D14 | SQL is submitted through the SQL Gateway | no embedded client in the image; the same SQL runs in production |
| D15 | The truth sink collapses duplicate keys in a batch | Postgres refuses an `ON CONFLICT` that touches the same key twice in one statement |
| D16 | 100 TPS and the ground truth as the oracle | the demo must be reproducible, and the ground truth is what makes the demo meaningful |

**The three that would be hardest to justify in a review**

* **D2 (dedup after the join)** — it looks wrong until you know that Flink's
  interval join requires an append-only input, so a retracting dedup cannot
  be joined against. The cost is that the join sees duplicates; the benefit
  is that the dedup is exact and simple.
* **D3 (event-time audit key)** — it is what turns "replay the stream" from a
  dangerous operation into a healing one.
* **D4 (monotonic serving table)** — it is the only reason a late-arriving
  *old* decision cannot corrupt the current state. A trigger is a better
  place for that rule than application code, because every writer is subject
  to it.

---

## 10. Known limits, stated honestly

### 10.1 The two that would embarrass a production deployment

1. **No `IN_TRANSIT` state (gap G1).** A settlement that clears the bank a
   day later is *normal*. The fast job calls it `MISSING_SETTLEMENT` once
   the 4-hour window closes, so the exception queue is full of false
   positives. The fix is an explicit in-transit state with a per-partner
   *expected clear date*, and `MISSING_SETTLEMENT` only after that date.
   This is the first thing a real deployment would need, and the thing the
   community is being asked about.
2. **No per-partner cutoff calendar (gap G2).** A partner whose batch rolls
   over at local midnight and converts to UTC afterwards broke reconciliation
   for 11 minutes a night. The fix is to store the partner's effective date,
   its processing window and our received-at as three separate fields, and to
   reconcile against the *contractual* arrival window rather than the clock.

### 10.2 The rest

* **The two sinks are not atomic with each other.** `match_results` and
  `decision_audit` are written by two independent JDBC sinks, so a crash
  between them can leave a decision without its audit row. The next replay
  heals it (the audit key is deterministic).
* **The Iceberg log grows** with the decision volume; compaction and expiry
  are a maintenance job, not part of the demo loop.
* **100 TPS.** The bounded join state is `rate × window`; 500 TPS needs more
  memory than an 8 GB laptop has. Raise `TPS` on a bigger machine.
* **One shared TaskManager** hosts both jobs. Splitting them is the obvious
  next step.
* **No FX** (out of scope for INR) and **no fee decomposition** (gap G3):
  fee differences are reported as `AMOUNT_MISMATCH`, not as a distinct
  reason.
* **Detection is arrival-driven.** A reconciliation that reacts to rows that
  show up can never notice a payment that *never* arrives. That needs an
  expected-arrival watermark per partner per day (gap G11).

---

## 11. Questions and answers

### A. The domain — payments and UPI

**A1. What is a UPI collection, in one sentence?**
A customer pays a merchant through the Unified Payments Interface; the
payment network (switch) authorises it, the merchant is told via a webhook,
and the money reaches the merchant's bank via a settlement file a day later,
net of fees.

**A2. What is an RRN, and why is it the natural key?**
The *Reference Retrieval Number*: the payment network's unique reference for
a transaction. It is the only identifier that all three legs can carry
without translation, so it is the join key here. Real integrations also need
an id-mapping layer (gap G8), because the order id, the bank's reference and
the PSP's transaction id are all different keys for the same payment.

**A3. Why does the settlement lag the switch?**
The switch authorises in real time; the money moves through the
inter-bank system and is batched into settlement cycles (four a day in this
model), then the bank credits it. So "money in transit" is a normal state
lasting hours, not an exception.

**A4. What is MDR, and why does it break naive matching?**
Merchant Discount Rate: the acquirer's fee, usually a percentage plus a
fixed component. The customer paid ₹1,499.00 and the settlement says
₹1,470.00 because ₹29 went to MDR. Comparing the switch amount to the
settlement amount without decomposing the fee makes every payment look
mismatched. This project does not yet decompose it (gap G3) — it reports the
difference as `AMOUNT_MISMATCH` and the ops advice says "check the MDR
deduction".

**A5. What is a UDIR dispute?**
The Unified Digital Payment Remittance and Settlement mechanism: the formal
channel for raising a dispute on a payment that was debited but not settled
or not credited. It is what the exception queue tells ops to do once a
`MISSING_SETTLEMENT` is older than 24 hours.

**A6. What is a chargeback, and when does this pipeline use one?**
A reversal initiated through the card/PSP network. Here it is the ops
action for an unexplained `AMOUNT_MISMATCH`: check the fee deduction first,
and if it does not explain the difference, raise a chargeback carrying the
`bank_ref` so the bank can trace it.

**A7. Why is the callback window symmetric (±2 minutes)?**
Because merchant servers have skewed clocks. A webhook that says 09:59:50
for a 10:00:00 switch event is *late but correct*, and a one-sided window
would call it missing. The `OUT_OF_ORDER` fault class exists precisely to
test this: the verdict must stay `MATCHED`.

**A8. Why is the settlement window one-sided (`[switch_ts, switch_ts + 4 h]`)?**
Because a settlement that *predates* the switch is not a settlement of that
payment — it is somebody else's money, or a re-used reference. Accepting it
would create false `MATCHED` verdicts.

**A9. What is a settlement cycle, and does the model use one?**
Yes: the generator labels each settlement with an NPCI-style cycle id
(`YYYY-MM-DD-Cn`, four cycles a day). It is part of the event shape rather
than of the decision logic, but it is what a cutoff calendar (gap G2) would
key on.

**A10. What would a real deployment need that this does not have?**
In rough priority order: the `IN_TRANSIT` state with expected clear dates
(G1), a per-partner cutoff calendar (G2), fee decomposition (G3), break
ownership and escalation (G4), reason codes distinct from status (G5), a
clearing account and a GL tie-out (G6), decision evidence (G7), id mapping
(G8) and an expected-arrival watermark (G11).

### B. Explaining the technology

**B1. Why Flink, and not a plain Kafka consumer?**
Because the decision depends on *time* and *bounded memory*: the job must
join a record with events that arrive later, remember them for a bounded
window, and emit a verdict only when the watermark says the window is
closed. A hand-written consumer would need a timer service, a state store
with expiry, and exactly-once handling — that is what Flink already is.

**B2. Why Flink SQL rather than the DataStream API?**
The whole decision is two `JOIN`s and a `CASE`. In SQL it is reviewable,
and it is the same artifact that runs in production (submitted through the
SQL Gateway, ADR D14). The cost is that some streaming patterns (custom
state, complex async I/O) are awkward in SQL.

**B3. What is `EXECUTE STATEMENT SET` and why is it used?**
Flink's multi-sink construct: several sink definitions are executed as one
graph, so all five sinks see the same records in a single job and a single
checkpoint. Without it, each sink would be a separate job, each with its own
checkpoint, and the sinks could disagree about what was processed.

**B4. What is a watermark, again, in one sentence?**
A promise from the source that no more events with a timestamp older than
this will arrive, which is what lets the engine close windows and expire
join state.

**B5. Why is the callback window ±2 minutes but the settlement window 4
hours?**
Because the two legs have different physics. A callback is a webhook: it
arrives in seconds, and its only failure mode is a clock skew or a missing
delivery. A settlement is a banking process: it arrives in hours, by
definition. Using one window for both would either drop skewed callbacks or
keep 24 hours of state to notice a missing callback.

**B6. What is an interval join?**
A temporal join: rows are paired only if their event-time intervals overlap
(or, here, fall within a configured tolerance). Flink keeps the left side in
state for the duration of the window, which is exactly the bounded state
this project is about.

**B7. What does "bounded state" buy, and what does it cost?**
It bounds memory (so the job fits on a laptop) and it defines the SLA: the
window *is* the promise you make about how long you will wait for a leg.
The cost is that a leg arriving after the window is invisible to that job —
which is why the repair job exists.

**B8. Why is the dedup after the join?**
Because a Flink interval join requires an append-only input; a deduplication
that retracts rows cannot be its input. So the join sees duplicates and the
dedup happens downstream with
`ROW_NUMBER() OVER (PARTITION BY rrn ORDER BY switch_ts ASC) = 1`. The
alternative (dedup first, with a retract stream) is not available on this
engine (ADR D2).

**B9. What is exactly-once, and does this project have it?**
Flink's checkpointing mode: the job's state and the sink offsets are
committed together, so a failure replays from the last checkpoint without
duplicating or losing effects. The sinks are idempotent (upserts on
deterministic keys), so "exactly once" is really "at least once + idempotent
sinks" — which is the honest way to state it.

**B10. What is a checkpoint, in plain words?**
A consistent snapshot of everything the job knows: the join state, the
offsets it has read, and the sinks' pending writes. If the job dies, it
restarts from the last snapshot instead of from the beginning. Here they are
every 30 s, and the state backend is RocksDB with incremental checkpoints
because the heap backend exhausted the heap and stalled the heartbeat.

**B11. Why RocksDB and not the heap state backend?**
RocksDB spills to disk, so state size is bounded by disk rather than by the
JVM heap. The heap backend ran out of memory and the heartbeat stalled —
which is the single most instructive operational failure in the log.

**B12. What is a Kafka consumer group, and why does each job need its own?**
A consumer group is the unit of partition assignment. Two jobs in the *same*
group would split the partitions between them, so each would see only part
of the stream — and the repair job would see almost no settlements. Hence
`recon-fast` and `recon-late` (ADR D13).

**B13. What is `earliest-offset` and what does it buy?**
The jobs start reading from the beginning of the topic. That makes a
resubmit a full replay: the state is rebuilt from scratch, and because the
audit key is deterministic, the replay is idempotent. Replay becomes a
recovery tool rather than a hazard.

**B14. What is Avro and why not JSON?**
A binary, schema-first format with a registry. The schema is part of the
contract, so a producer and a consumer that disagree fail loudly at
registration time instead of silently at runtime. The schema here is
deliberately flat: `rrn`, `merchant_id`, `amount_paise`, `status`,
`event_ts` (epoch millis, a plain `long`, so the contract does not depend
on a logical-type mapping).

**B15. What is Iceberg, and why not just a Postgres table?**
Iceberg is an open table format on object storage: partitioned, versioned,
and readable by many engines. It is the right home for an append-only
decision log — cheap to keep forever, cheap to re-read, not tied to one
database. Postgres is the right home for "current state" because it is
transactional and fast to query (ADR D5).

**B16. Why is the Iceberg table append-only when Iceberg supports
row-level updates?**
Because DuckDB's equality-delete index needed more than 2 GB for our volume.
Switching the deletes off keeps the write path simple and cheap, and the
"latest decision per RRN" rule moves to a two-line `ROW_NUMBER()` in the dbt
staging model (ADR D6).

**B17. What is dbt doing here, and why DuckDB?**
dbt turns the raw log into four marts (daily control totals, exception
queue, detection quality, detection precision) and 15 tests. DuckDB reads
the Iceberg snapshot directly through the REST catalog, so the gold layer is
built without a warehouse. dbt also means the finance numbers are versioned
and tested, not hand-written SQL.

**B18. Why is the KPI/alert window in *processing* time while the joins are
in event time?**
Because the two questions are different. "Did these three legs match?" is a
question about when things happened. "Is merchant M0001 failing right now?"
is a question about the operator's wall clock. Mixing them produces alerts
that fire at the wrong moment (ADR D7).

**B19. What is a `BEFORE UPDATE` trigger and why use one?**
A rule the database enforces on every write, before it happens. The
monotonic rule ("a row may only move to a decision with a later
`decision_ts`") is a *correctness invariant*, so it belongs in the database
rather than in each writer: a future writer cannot forget it, and a buggy
writer cannot bypass it (ADR D4).

**B20. What does the `recon.merchant_minute_stats` table actually hold?**
One row per merchant per processing-time minute: transactions seen, unmatched
count, and the match rate. The alert table is the subset where the unmatched
rate breached 10 % with at least 20 transactions in the window. The
generator deliberately concentrates faults on one merchant so this alert
actually fires in a demo.

**B21. Why is the project called "three-way"?**
Because it reconciles three legs of the same payment: switch, callback and
settlement. Two-way reconciliation (order vs settlement) is the common case;
the third leg — the merchant callback — is what makes "did the merchant
know?" answerable.

### C. Correctness, failure and recovery

**C1. What happens if the pipeline crashes mid-way?**
It restarts from the last checkpoint (30 s earlier), re-reads the affected
partitions and re-decides. Because the audit key `(rrn, decision_ts)` is
derived from event time, the replayed decisions land on the same keys and
upsert the same rows. A replay is a no-op — and it heals any gap a crash
left behind.

**C2. What if a stale decision arrives after a newer one?**
The Postgres trigger rejects it. The serving table is monotonic in
`decision_ts`, so a late-arriving *old* opinion cannot undo a correction
(ADR D4). The audit still keeps both.

**C3. Can a payment get two different final verdicts?**
Only one, because the serving table holds the latest decision per RRN and
the two jobs' windows are disjoint. The audit will show a sequence
(`MISSING_SETTLEMENT` → `MATCHED`), which is the truth, not a contradiction.

**C4. What if the same settlement arrives twice?**
The dedup on RRN keeps one row, so the verdict does not change and no
duplicate exception is created. There is an explicit test for this
(`test_duplicate_switch_does_not_create_extra_exception`).

**C5. What if the callback arrives before the switch?**
It still matches: the callback window is symmetric (±2 min), so a
clock-skewed callback is found. The `OUT_OF_ORDER` fault class asserts the
verdict stays `MATCHED`.

**C6. How do you know a missing settlement is really missing, and not just
late?**
You cannot, at 4 hours — that is exactly the false-positive problem (G1).
What the pipeline guarantees is that the claim is *time-bounded*: it is
emitted only after the settlement watermark passes `switch_ts + 4 h`, and
the repair job will correct it if the settlement turns up within 24 hours.
The honest fix is a per-partner expected clear date.

**C7. What does "0 false positives" mean here, and what does it not mean?**
It means: of all the exceptions the pipeline raised, every one corresponded
to a fault the generator actually injected — no invented breaks, no
duplicate-derived noise, no callback-window artefacts. It does **not** mean
the pipeline is right about the real world; the generator is the only
source of truth available here, so recall and precision are measured against
the simulator, not against production data.

**C8. What happens if the *generator* is wrong?**
Then the oracle is wrong too, and the scores are meaningless. That is why
the oracle is a separate module with its own unit tests
(`generator/model.py` is dependency-free and fully unit-tested), and why it
is published on a separate topic and written by a separate process — if the
reconciler is broken, the truth must not be (ADR D8).

**C9. What does the "control total difference ₹0.00" prove?**
That for the sampled day and merchant, gross value equals reconciled +
mismatch + outstanding + missing callback. It is a conservation check on the
*reporting*, not on the streaming: it catches a status that was counted in
the wrong bucket, an amount written twice, or a join that dropped a leg. It
is the identity finance would audit anyway.

**C10. Why is `mismatch_inr` the full switch value rather than the
difference?**
Because the difference would make the control total look balanced while the
money is still unresolved. `mismatch_inr` is the money under investigation
(the full transaction value); `mismatch_delta_inr` is the amount by which
the legs differ. The first is what finance escalates, the second is what
ops debugs.

**C11. What happens to a payment that is still unresolved after 24 hours?**
The fast job said `MISSING_SETTLEMENT`; the repair job (window 4–24 h) will
not see a settlement that never arrives, so the row stays as an exception
and the ops advice escalates to a UDIR dispute. There is no automatic
"expired" state — a real system would add one.

**C12. What is the worst failure this design can have?**
A systematic false negative: a real mismatch that the pipeline never
reports. The defences are (a) the fault matrix covers six distinct failure
shapes, (b) the oracle scores every run, (c) the CI job fails the build on
recall movement, and (d) the *absence* case is currently the weak spot —
arrival-driven detection cannot see a payment that never arrives (G11).

### D. Design and trade-offs

**T1. Why two jobs instead of one job with a 24-hour window?**
A single job with 24 h of state would hold 6× the memory and would delay
*every* "missing" verdict until the 24-hour window closed. The two-job split
keeps the common case fast (minutes) and pushes the rare late case into a
cheaper, slower job whose window is exactly the complement. The cost: two
jobs, two consumer groups, two sets of state, and a brief period where the
verdict is knowingly wrong (it is corrected later, and the audit shows both).

**T2. Why is the "repair" job an `INNER JOIN` while the fast job is a
`LEFT JOIN`?**
Because absence means different things in the two. For the fast job, a
missing settlement *is* the signal (`MISSING_SETTLEMENT`). For the repair
job, a missing settlement means nothing — it only has work to do when a
settlement exists. An `INNER JOIN` makes it structurally impossible for the
repair job to invent a decision.

**T3. Why not use machine learning / fuzzy matching for the joins?**
Because the join key is exact (the RRN) and fuzzy matching would make the
decision non-deterministic and unexplainable. An auditor asking "why was
this payment marked mismatched?" needs an answer, not a score. The
deterministic core is the product; a model can only propose.

**T4. Why not simply run the batch job every hour?**
Because batch answers "what did we earn" and not "is this payment OK?". The
ops question is per-payment and immediate: a customer was debited at 10:00
and it is 10:30 and the merchant is asking. Hourly batch still gives a
30-minute-old answer about a 30-minute-old question.

**T5. Why does the design need both a serving store and a log?**
They answer different questions. "What is the state of R1 right now?" is a
hot, single-row read — Postgres. "What did we decide about R1 last Tuesday
and why?" is a historical scan — the Iceberg log, which is append-only,
partitioned, and cheap. Collapsing them into one store is possible but
either makes the hot path slow or the history expensive.

**T6. What would you change if this had to run at 10,000 TPS?**
Not the shape: the two-window design scales, the state is `rate × window`
and would be sharded across more parallelism and more TaskManagers. What
changes: the source parallelism (one partition per topic is a demo
artefact), the checkpoint interval and the Iceberg commit cadence, the
sink batching, and the need for a real key-value or state backend for the
join state. The decision logic itself would not change, which is the point
of keeping it in SQL.

**T7. Why does the project use Flink at all, given a simpler batch would
pass the same tests?**
Because the interesting failure modes are streaming failure modes: late
data, watermark behaviour, state expiry, replay safety, correction
ordering. A batch implementation cannot exercise any of them, and the
questions the community actually asks ("how do you know the payment is OK
now?") are streaming questions.

**T8. Why is the state backend decision (RocksDB vs heap) worth an ADR?**
Because it is a constraint, not a preference: the heap backend exhausted the
JVM heap and stalled the heartbeat, which looked like a hang and cost hours.
Recording it in the ADR tells the next person that the state size is not
negotiable — it is bounded by design, and the backend must match it.

**T9. If you could keep only one part of this project, what would it be?**
The **oracle + the CI job**: a pipeline that ships with an executable claim
about its own correctness, re-verified on every commit. The streaming
implementation is replaceable; the discipline of measuring detection
quality against ground truth is not.

**T10. What is the strongest criticism of this design?**
That the 4-hour window is arbitrary and encodes a false promise
("missing" means "not yet", but we call it "missing"). It is a *labelled*
false positive in the demo, and a real deployment would be wrong every day
in production. The honest fix (G1) is known, specified, and not yet built —
the reason it is still open is that the right answer is per-partner and
therefore a data problem, not a code problem.

**T11. Why is `DUP_SWITCH` expected to end as `MATCHED` and not
`DUPLICATE`?**
Because a retry of the switch record is a transport artefact, not a
business fact. The payment happened once; the customer paid once. The
verdict should describe the payment's state, not the transport's noise.
Duplicates are counted (in the audit, and in the metrics) but they do not
change the customer's truth.

**T12. Why keep the decision in a `CASE` expression rather than a table of
rules?**
Because a table of rules would need to be loaded into the job and would
invite per-merchant configuration, which is a product decision. The `CASE`
is fixed, reviewable, mirrored in Python for the tests, and identical in
the two jobs. Per-merchant rule configuration is a future problem — and one
of the reasons people say a fixed reason-code taxonomy eventually ossifies.

### E. Scale, cost and performance

**E1. What throughput does it sustain?**
~100 TPS on a laptop, with decisions at ~37 s average latency (p50 27 s,
p95 54 s) measured on the published CI run, over 183,205 decisions.

**E2. What dominates the memory?**
The bounded join state: `rate × window × bytes per record`. At 100 TPS with
a 4-hour window that is ~1.44 M switch records; the 24-hour repair job holds
6× that. This is why the state backend is RocksDB and why parallelism is 1
(ADR D10).

**E3. What would it cost to run this in production?**
A Flink cluster (or managed equivalent), Kafka, a Postgres, an object store
and the dbt runs — no more than a normal streaming deployment. The project
deliberately avoids Airflow, Spark and Debezium: one job each for generation
and reconciliation, and no CDC layer, because the sources are events
already.

**E4. How do you scale the sink?**
The Postgres sinks are JDBC upserts; at production volume you would batch
them (the JDBC connector's `sink.buffer-flush` settings) and partition by
RRN to avoid hot rows. The Iceberg sink is already partitioned by decision
day, which is the right axis for a log that is read by time range.

**E5. What is the cheapest possible version of this idea?**
Hourly batch, four statuses, a spreadsheet of exceptions, and one
id-mapping table. That covers maybe 70 % of the pain for 5 % of the work —
which is what one commenter in the PSP thread said, and it is the right
advice for a mid-market team. The streaming version earns its cost only
where the ops question is genuinely "is this payment OK *right now*".

**E6. What breaks first if you double the rate?**
Memory, not CPU: the join state doubles, the checkpoint size doubles, and
the RocksDB write amplification grows. The fix is more parallelism and
more TaskManagers, not a different design.

### F. Security, privacy, compliance and audit

**F1. What personal data is in the stream?**
In this reference implementation, none: the Avro contract is
`rrn`, `merchant_id`, `amount_paise`, `status`, `event_ts`. In a real
deployment you would add the payer identifier, and then the whole thing
becomes a data-protection problem: retention limits, access control,
encryption at rest, and a deletion path that also has to reach the
append-only log (Iceberg supports row-level deletes and partition
rewrites; that is a real cost of an append-only design).

**F2. Is the audit trail sufficient for an auditor?**
It records what was decided and when (in event time), which answers "what
did the system know and when". It does not record *which rows supported the
decision* — gap **G7**. A decision-evidence table (which switch/callback/
settlement rows justified the verdict) is what an auditor would actually
ask for.

**F3. Why is the audit keyed by event time rather than wall clock?**
Two reasons. Idempotency: a replay must land on the same key. And truth: the
question an auditor asks is "when did we know this?", not "when did the
process write the row" — and the answer to the first is the event time of
the leg that completed the decision.

**F4. What about money movement itself?**
The pipeline never moves money. It decides and reports. That is deliberate:
the blast radius of a bug is a false exception, not a wrong transfer.

**F5. Does the design satisfy "no silent corrections"?**
Yes: corrections are *appends*. A correction creates a new audit row with a
later `decision_ts`; nothing is overwritten or deleted. A reader that wants
the current truth takes the latest row; a reader that wants the history
takes all of them.

**F6. What about the double-writes to Postgres and Iceberg?**
They are not atomic (a crash between them can leave a decision without its
audit row). The recovery is a replay, which is safe by construction. A
strictly atomic design would need a single store or a distributed
transaction — the current trade-off was chosen for operational simplicity,
and it is documented as a known limitation rather than hidden.

### G. How this was built

**G1a. Was AI used?**
Yes, throughout: code, SQL, infrastructure, and these documents. The
project's own posts say so up front, and the README's own framing is
"reference implementation to argue with, not production code". The claim in
the README — 100 % recall, 0 false positives — is not an AI assertion; it is
re-measured by the CI job on every run from the published artifacts.

**G2a. What failed while building it?**
29 documented failures in [FAILURE_LOG.md](FAILURE_LOG.md), each with the
exact error, the root cause, the fix and how it was verified. The
interesting ones: a missing Hadoop jar in the connector image, a Kafka broker
that starved the settlement watermark (so the job looked hung), a minio-init
container that never completed, an edge-runtime function that could not
reach github.com, and the heap state backend that exhausted the heap and
stalled the heartbeat.

**G3a. Why write the failure log at all?**
Because the failures are the most useful part of the project for a reader who
wants to build the same thing. A design doc describes the happy path; the
failure log shows which parts of the happy path are load-bearing and what
they cost. It is also how the ADR list grew: most ADRs exist because
something broke.

**G4a. How do you keep the docs from rotting?**
The documentation site is generated from the markdown in the repository, so it
cannot drift from the source; every push to `main` rebuilds and redeploys it.
The published metrics come from the same CI job, so the numbers in the
README are the numbers of the last run.

**G5a. What is the test strategy in one sentence?**
The decision table and the fault semantics are pure functions and are unit
tested without Docker (33 tests); the streaming behaviour is asserted
against the running stack (8 integration tests, with age filters so that
late-arriving data does not produce false failures); and the reporting layer
is asserted in dbt (23 nodes) — with the ground truth as the oracle for all
three levels.

### H. Product and market

**H1. Who is this for?**
A team that already has the payments (a PSP, a marketplace, a merchant with
several acquirers) and is currently reconciling in spreadsheets. The
reference implementation is a starting point, not a product.

**H2. What does the market research say is missing?**
Ten gaps (G1–G10, plus G11 added after the engagement threads), of which the
sharpest are the in-transit state, the per-partner cutoff calendar, fee
decomposition, break ownership, reason codes distinct from status, a
clearing account with a GL tie-out, decision evidence, id mapping and the
expected-arrival watermark. See [MARKET_RESEARCH.md](MARKET_RESEARCH.md).

**H3. Who is already doing this?**
Stripe/Adyen Sigma (event-level exports, tied to their own rails),
Hyperline, BroSettlement, kosha.finance (commercial products), Zingg
(record matching, open source, fuzzy), and Reconify (open source,
deterministic). This project is not novel as a product; its value is the
inspectable reference and the evidence.

**H4. If you had to pick one thing to build next, what?**
The `IN_TRANSIT` state with per-partner expected clear dates (G1), because
without it the exception queue is full of false positives and the whole
premise — "an exception means something" — collapses. It also unlocks G2
(cutoff calendar) and G11 (expected-arrival watermark), which share the same
data.

**H5. What is the strongest argument *against* this project?**
That streaming reconciliation is a solution looking for a problem: if your
ops team reconciles daily, a good batch job with a good id-mapping table
does the same thing for a fraction of the complexity, and a false-positive
exception queue is worse than a small spreadsheet. The project is only
justified where the per-payment, real-time question is real — and even then,
the honest version of the answer is "most of the value is in the id mapping
and the fee decomposition, not the streaming".

**H6. What would make this a product rather than a reference?**
A per-partner configuration model (expected clear dates, cutoffs, fee
schedules), a reason-code taxonomy that merchants can extend, a GL/clearing
account integration, a UI for break ownership and escalation, and — the
uncomfortable one — a support model for "your PSP changed its CSV header
again". The last one is the real product: reconciliation is a permanent
maintenance burden, and the market is full of people paying for exactly
that.

---

## 12. Questions to ask other people

These are the open design questions this project genuinely does not have an
answer for — the ones worth asking a practitioner. They are the same
questions put to the community, and each maps to a gap in §10.1.

| # | Question | Gap | Why it is hard |
|---|---|---|---|
| Q1 | Do you model an explicit `IN_TRANSIT` state with a per-partner expected clear date, or do you keep "missing" and age the break? | G1 | "missing" is wrong on every ordinary payment |
| Q2 | Do you keep a cutoff calendar per integration, or reconcile against the contractual arrival window rather than the clock? | G2 | partners cut over at their own local midnight |
| Q3 | Is a fixed reason-code taxonomy worth building, or does it ossify per merchant? | G5/G10 | a taxonomy that cannot express a partner's quirk gets ignored |
| Q4 | Who owns a break, and what is its expected clear date? | G4 | an exception sheet without an owner is a list, not a queue |
| Q5 | Do you post gross + fee to a clearing account per processor and let the bank deposit clear it? | G6 | finance reconciles to the ledger, not to the transaction |
| Q6 | Do you keep the *evidence* (which rows supported the decision) or only the decision? | G7 | auditors ask for the former |
| Q7 | How do you map the switch reference, the order id and the bank reference? | G8 | they are three different keys for one payment |
| Q8 | How do you detect a payment that *never* arrives, rather than one that arrives wrong? | G11 | arrival-driven reconciliation is structurally blind to it |
| Q9 | How do you decompose a settlement line into gross, fee, tax and net? | G3 | without it every match looks like a mismatch |
| Q10 | Do you split the fast and the slow path into two jobs, or keep one long window? | — | latency vs state vs completeness |

---

## 13. Self-check quiz

If you can answer these without looking, you understand the project.

1. Which leg wins when the three legs disagree, and why?
2. What does the 4-hour window actually *mean*, operationally?
3. Why does a late settlement produce two audit rows and one serving row?
4. What is the difference between `decision_ts` and `decided_at`, and why does the choice matter?
5. Why can a replay not corrupt the serving table?
6. What is the memory formula, and what happens to it if you double the rate?
7. What does the oracle protect against, and where does it live?
8. Why is `LATE_SETTLE`'s *initial* recall 0 % by design?
9. What does the control-total identity check, and what would a non-zero difference mean?
10. Why is the settlement join one-sided and the callback join symmetric?
11. Which two gaps would you fix first in a real deployment, and what would each need?
12. What is the one thing in this project that no commercial competitor ships?

<details><summary>Answers</summary>

1. The **settlement** leg, because it is the money leg: if the money did not arrive, nothing else matters.
2. It is the maximum time the fast job will wait for a settlement before declaring `MISSING_SETTLEMENT` — and, because the state is bounded by it, the maximum memory the job needs. The window *is* the SLA *and* the memory budget.
3. The audit is keyed on `(rrn, decision_ts)`, and the two decisions have different event times (switch time vs settlement time), so both are kept. The serving table has one row per RRN and is monotonic, so it holds the latest.
4. `decision_ts` is the *event* time the decision is anchored to; `decided_at` is when the pipeline wrote it. Keying on event time makes a replay idempotent and makes the audit answer "when did we know this?".
5. Because the serving table is monotonic in `decision_ts` and enforced by a trigger: a stale write is rejected by the database.
6. `rate × window × bytes per record`. Doubling the rate doubles the state and the checkpoint size — the fix is more parallelism/TaskManagers, not a new design.
7. It protects the *measurement* against the thing being measured. It lives outside the Flink path (its own topic, its own sink) so a broken reconciler cannot corrupt the truth.
8. Because the fast job's window closes before the settlement arrives, so it cannot see it. The repair job (4–24 h) is what closes the gap, and *final* recall is 100 %.
9. That gross value = reconciled + mismatch + outstanding + missing callback, per day and merchant. A non-zero difference means a rupee is unaccounted for — a mis-bucketed status, a double-written amount, or a dropped leg.
10. A settlement that predates the switch is not that payment's settlement, so accepting it would create false `MATCHED`s. A callback can legitimately carry a skewed clock, so its window is symmetric.
11. G1 (in-transit state + per-partner expected clear date) and G2 (per-partner cutoff calendar). Both need per-partner configuration data, which is why they are the same piece of work.
12. The **evidence**: a ground-truth oracle plus a CI job that re-measures detection quality on every run and fails the build if it moves. Everyone ships a dashboard; almost nobody ships the claim that the dashboard is right.

</details>

---

## 14. Glossary

| Term | Meaning here |
|---|---|
| **RRN** | Reference Retrieval Number — the network's unique reference for a payment; the join key |
| **leg** | one of the three records of a payment: switch, callback, settlement |
| **switch** | the payment network that authorises the payment |
| **callback** | the merchant's webhook telling the merchant the payment succeeded |
| **settlement** | the bank file crediting the money, net of fees, usually a day later |
| **MDR** | Merchant Discount Rate — the acquirer's fee |
| **UDIR** | the UPI dispute/settlement mechanism for debited-but-unsettled payments |
| **chargeback** | a reversal of a payment through the network |
| **event time** | when the event happened, per its own timestamp |
| **processing time** | when our system processed the event |
| **watermark** | the engine's promise that no older events will arrive; drives window closing |
| **interval join** | a temporal join that pairs rows only if their timestamps are within a tolerance |
| **bounded state** | the retained state a job needs, bounded by `rate × window` |
| **tumbling window** | a fixed, non-overlapping time bucket used for the KPIs and alerts |
| **checkpoint** | a consistent snapshot of state + offsets, taken every 30 s |
| **exactly-once** | checkpointing mode where state and sink offsets commit together |
| **idempotent** | re-applying the same operation leaves the same result |
| **monotonic** | a row may only move forward in decision time (enforced by a trigger) |
| **upsert** | insert, or update the existing row with the same key |
| **audit key** | `(rrn, decision_ts)` — the deterministic identity of a decision |
| **oracle / ground truth** | the record of what fault was injected, used to score the pipeline |
| **recall** | of the faults injected, the share the pipeline detected |
| **precision** | of the exceptions raised, the share that were genuine faults |
| **control total** | gross = reconciled + mismatch + outstanding + missing callback |
| **outstanding** | authorized but not yet settled money (the in-transit bucket) |
| **fault class** | one of the six injected failure shapes (`DUP_SWITCH`, `AMOUNT_DRIFT`, `MISSING_SETTLE`, `LATE_SETTLE`, `MISSING_CALLBACK`, `OUT_OF_ORDER`) |
| **initial vs final verdict** | the fast job's first verdict vs the verdict after the repair job |
| **Iceberg** | an open table format on object storage; here the append-only decision log |
| **dbt** | the transformation/testing layer that builds the gold marts |
| **DuckDB** | the in-process engine dbt runs here, reading the Iceberg snapshot |
| **NPCI** | the National Payments Corporation of India; the settlement files come from there |
| **TIME_SCALE** | the knob that compresses both the delays and the timestamps for the demo |

---

## 15. Where to go deeper

| Document | What it adds to this guide |
|---|---|
| [ARCHITECTURE.md](ARCHITECTURE.md) | the exact SQL, the data model of every table, the state and checkpointing details |
| [DECISIONS.md](DECISIONS.md) | all 16 ADRs with context, choice, trade-off and cost |
| [FAILURE_LOG.md](FAILURE_LOG.md) | 29 failures with root causes — the most useful document for anyone building this |
| [OPERATIONS.md](OPERATIONS.md) | the resource envelope, the tuning knobs, the troubleshooting matrix |
| [VERIFICATION.md](VERIFICATION.md) | what every test asserts and the measured results |
| [MARKET_RESEARCH.md](MARKET_RESEARCH.md) | what practitioners say, and the gap list this project has not closed |
| [REDDIT_POSTS.md](REDDIT_POSTS.md) | the drafts used to ask the community the open questions in §12 |

**Code map for a reader who wants to read the implementation**

| If you want to understand… | Read |
|---|---|
| the decision rules | `flink/20_recon.sql` (the `CASE`) and `tests/test_decision_logic.py` (the same rules in Python) |
| the fault semantics | `generator/model.py` — dependency-free, unit-tested, no Kafka anywhere in it |
| the late-correction trick | `flink/30_late_correction.sql` — the `(4 h, 24 h]` window and the `INNER JOIN` |
| the audit and the monotonic rule | `postgres/init/01_recon.sql` — the unique key and the `BEFORE UPDATE` trigger |
| the reporting | `dbt/models/marts/*.sql` — the exception queue, the control totals, the quality report |
| the time compression | `flink/render_sql.sh` — how production units become demo units |
| the recovery story | `flink/submit.sh` and `Makefile` — cancel, resubmit, replay, and why the replay is safe |
