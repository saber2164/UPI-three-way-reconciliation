# UPI three-way reconciliation

[![simulation](https://github.com/saber2164/UPI-three-way-reconciliation/actions/workflows/simulation.yml/badge.svg)](https://github.com/saber2164/UPI-three-way-reconciliation/actions/workflows/simulation.yml)
[![docs](https://img.shields.io/badge/docs-live%20site-6ea8fe)](https://upi-three-way-reconciliation.vercel.app)

Real-time reconciliation of UPI collections across the three systems that
record them: the **switch** log, the **merchant callback** and the **bank
settlement**. For every payment the pipeline emits exactly one decision —
`MATCHED`, `AMOUNT_MISMATCH`, `MISSING_SETTLEMENT` or `MISSING_CALLBACK` —
within minutes, plus per-merchant health, an actionable exception queue and
daily finance control totals.

Today this is done in a T+1 batch, which means customers are debited without a
confirmation and finance finds shortfalls a day late. This project moves the
decision into the stream and keeps a replayable audit trail of it.

```
generator (100 tps, 6 injected fault classes, ground truth on a side topic)
   │  Avro + Schema Registry, idempotent producer
   ▼
Kafka: upi.switch · upi.callbacks · upi.settlements · recon.ground_truth
   │
   ▼
Flink SQL (exactly-once, 30 s checkpoints, RocksDB state)
   ├── fast job        4 h bounded state  → decision + audit + KPIs + alerts
   └── repair job     24 h bounded state  → corrects late settlements
   │
   ├──► Postgres recon.*        serving store  (one row per RRN, current state)
   ├──► Iceberg lake.recon.*    durable log    (one row per decision)
   └──► dbt (DuckDB)            gold layer     (daily recon, exception queue, recall)
```

* **Stack**: Kafka · Flink 1.19 (SQL) · Iceberg (REST catalog on MinIO) ·
  Postgres 16 · dbt on DuckDB · Streamlit. No Airflow, no Spark, no Debezium.
* **Footprint**: 11 containers, ~7 GB of Docker memory, runs on a laptop.
* **Measured**: 100 % final recall on all six fault classes, 0 false positives,
  0 regressions, control totals balanced to ₹0.00, ~100 decisions/s sustained.

---

## Quick start

```bash
git clone https://github.com/saber2164/UPI-three-way-reconciliation.git
cd UPI-three-way-reconciliation
cp .env.example .env

make up          # build + start everything, submit the two Flink jobs
make status      # jobs, decision counts, late corrections
open http://localhost:8503   # live dashboard
```

The first build takes a few minutes (Flink connector image, Python images).
Decisions appear after about a minute of traffic. The `LATE_SETTLE` repair needs
the most delayed settlement, i.e. ~20 real minutes at the default 60× time
scale.

```bash
make dbt              # build the gold layer against the pinned Iceberg snapshot
make test             # unit tests, no Docker required
make integration-test # needs the stack up and ~25 min of traffic
make verify           # both test suites
make submit           # cancel and resubmit both Flink jobs
make logs SERVICE=flink-taskmanager
make down             # stop, keep the data
make clean            # stop and delete the volumes
```

| Service | URL |
|---|---|
| Streamlit dashboard | http://localhost:8503 |
| Flink UI | http://localhost:8087 |
| Flink SQL Gateway | http://localhost:8086 |
| Kafka (external listener) | `localhost:19093` |
| Schema Registry | http://localhost:8085 |
| Postgres | `postgresql://app:app@localhost:5432/fintech` |
| MinIO console / S3 | http://localhost:9001 / http://localhost:9000 (`minio` / `minio12345`) |
| Iceberg REST | http://localhost:8181/v1/config |

---

## How the decision is made

Three legs describe the same payment. The pipeline joins them in event time
and applies a fixed precedence — **the settlement leg wins**, because it is the
money leg:

| settlement | callback | amounts | decision | ops action |
|---|---|---|---|---|
| missing | any | — | `MISSING_SETTLEMENT` | wait for the next cycle; > 24 h → raise a UDIR dispute |
| present | missing / NACK | — | `MISSING_CALLBACK` | re-fire the webhook ×3, then refund |
| present | ACK | any leg differs | `AMOUNT_MISMATCH` | check MDR/fee deduction, else chargeback with `bank_ref` |
| present | ACK | all equal | `MATCHED` | — |

The interesting part is *when* a leg is allowed to be missing:

* The **fast job** keeps 4 h (scaled: 4 min) of state. When the settlement
  watermark passes `switch_ts + 4 h` and no settlement arrived, the row is
  emitted as `MISSING_SETTLEMENT`. No custom timers, no state store to operate.
* A settlement that arrives *after* that bound is invisible to the fast job —
  by design, that is what the bounded state is for. The **repair job** keeps
  24 h (scaled: 24 min) and joins only the settlements in `(4 h, 24 h]`, then
  upserts the same RRN to `MATCHED`.

That split is the central trade-off of the project: latency against
completeness, with a bounded state on both sides.

---

## The fault matrix

The generator injects six fault classes, all of them things that actually
happen in production:

| Injected fault | What the pipeline sees | Expected first status | Expected final status |
|---|---|---|---|
| `DUP_SWITCH` | the same RRN twice in `upi.switch` | `MATCHED` | `MATCHED` |
| `AMOUNT_DRIFT` | the settlement amount differs | `AMOUNT_MISMATCH` | `AMOUNT_MISMATCH` |
| `MISSING_SETTLE` | no settlement row at all | `MISSING_SETTLEMENT` | `MISSING_SETTLEMENT` |
| `LATE_SETTLE` | the settlement arrives after the 4 h bound | `MISSING_SETTLEMENT` | `MATCHED` (repair job) |
| `MISSING_CALLBACK` | no merchant webhook | `MISSING_CALLBACK` | `MISSING_CALLBACK` |
| `OUT_OF_ORDER` | the callback timestamp precedes the switch | `MATCHED` | `MATCHED` |

20 % of the traffic and 35 % of the faults are concentrated on merchant
`M0001`, so the merchant-level alert (> 10 % unmatched in a 2-minute window)
actually fires instead of being averaged away. Every 500th transaction gets a
deterministic round-robin fault, so each class appears even in a short run.

The **oracle** (`recon.ground_truth`) is deliberately outside the Flink path: if
the reconciler is wrong, the oracle must still be right. It is published to
Kafka by the generator and upserted into Postgres by a separate sink.

---

## Documentation

📖 **The documentation site is deployed at
[https://upi-three-way-reconciliation.vercel.app](https://upi-three-way-reconciliation.vercel.app)**
— it is generated from the markdown in this repository, so it can never drift
from the source.  Every push to `main` rebuilds and redeploys it.

| Document | What it covers |
|---|---|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | The event-time model, the joins, the sinks, the data model of every table, the state and checkpointing |
| [docs/DECISIONS.md](docs/DECISIONS.md) | Every design decision as an ADR: context, choice, trade-off, what it costs |
| [docs/FAILURE_LOG.md](docs/FAILURE_LOG.md) | What broke while building this, the exact error, the root cause, the fix and how it was verified |
| [docs/OPERATIONS.md](docs/OPERATIONS.md) | The resource envelope, the tuning knobs, the troubleshooting matrix, the reset procedures |
| [docs/VERIFICATION.md](docs/VERIFICATION.md) | What the tests assert and the measured results |
| [docs/MARKET_RESEARCH.md](docs/MARKET_RESEARCH.md) | What practitioners say about payment reconciliation, and the ten gaps the research exposed |
| [docs/REDDIT_POSTS.md](docs/REDDIT_POSTS.md) | The drafts for asking that community — a post and five replies, none of them posted |

The [failure log](docs/FAILURE_LOG.md) is the one to read if you want to know
what this cost: 29 distinct failures, from a missing Hadoop jar to a Kafka
broker that starved the event-time watermark.

The [market research](docs/MARKET_RESEARCH.md) is the external check: it finds
the same three-leg problem described by people running these systems today, and
it lists the ten things this project still does not do — the sharpest being a
"money in transit" state and a per-partner cutoff calendar.

---

## Repository layout

```
.
├── docker-compose.yml        the standalone stack (11 services)
├── Makefile                  up / down / status / dbt / test / submit
├── flink/
│   ├── 00_catalogs.sql       Iceberg REST catalog + session settings
│   ├── 10_sources.sql        Kafka sources, watermarks, the four sinks
│   ├── 20_recon.sql          the fast job: joins, dedup, decision, fan-out
│   ├── 30_late_correction.sql the repair job: the late window and the fix
│   ├── render_sql.sh          renders production time units into demo units
│   └── submit.sh              submits the SQL through the SQL Gateway
├── generator/
│   ├── model.py              the fault/decision model, dependency-free
│   ├── schemas.py            the Avro contracts
│   ├── txn_generator.py      the producer
│   └── truth_sink.py         the oracle sink
├── dashboard/app.py          the live Streamlit dashboard
├── dbt/                      the gold layer (DuckDB engine, 4 marts, 4 tests)
├── postgres/init/01_recon.sql the schema, the uniqueness constraints, the trigger
├── platform/flink/Dockerfile the Flink connector image
├── scripts/iceberg_maintenance.py  compaction / expiry / orphan removal (needs Spark)
├── tests/                    unit and integration tests
└── docs/                     the documentation set
```

---

## The shape of the data

| Store | Grain | Written by | Purpose |
|---|---|---|---|
| `recon.match_results` (Postgres) | one row per RRN | both jobs, upsert | current state; monotonic in decision time |
| `recon.decision_audit` (Postgres) | one row per RRN **and** decision | both jobs, upsert on `(rrn, decision_ts)` | the audit trail of every decision |
| `recon.merchant_minute_stats` (Postgres) | merchant × minute | fast job | operational health |
| `recon.alerts` (Postgres) | merchant × minute | fast job | merchant-level breaches |
| `recon.ground_truth` (Postgres) | one row per injected fault | the truth sink | the oracle for recall/precision |
| `lake.recon.match_results` (Iceberg) | one row per **decision** | both jobs, append-only | the durable, replayable log |

Postgres is the *serving* store (one row per RRN, current state, monotonic);
Iceberg is the *durable* log (every decision, append-only, partitioned by
decision day). The dbt staging model applies the "latest decision per RRN"
rule that the Iceberg equality deletes would otherwise encode — see
[decision D6](docs/DECISIONS.md) for why equality deletes are switched off.

---

## Time scaling

Waiting four hours for a settlement is not a demo. `TIME_SCALE` compresses
both the delays **and** the event timestamps, so Flink's event-time joins and
the generator agree on ordering.

| Production concept | `TIME_SCALE=60` (default) |
|---|---|
| Settlement bound (4 h) | 240 s |
| Late-settlement repair bound (24 h) | 1,440 s |
| Callback match window (±2 min) | ±2 s |
| Switch/callback watermark (30 s) | 1 s |
| Settlement watermark (5 min) | 5 s |
| Source idle timeout (30 s) | 1 s |
| KPI tumble (1 min) | 1 min (processing time) |
| Alert tumble (2 min) | 2 min (processing time) |

The SQL files stay in production units; `flink/render_sql.sh` substitutes the
scaled values at submit time. Use `TIME_SCALE=1` for production timing.

---

## Where it runs

| deployment | how |
|---|---|
| **your laptop** | `make up` — the whole stack in Docker, ~7 GB of memory |
| **GitHub Actions, no server at all** | the `simulation` workflow runs the whole stack on a runner every 30 minutes, validates it and publishes the metrics to the `runs` branch |
| **Vercel (the dashboard)** | `/dashboard` reads the published run through `api/snapshot.js` — the deployment is already live at [upi-three-way-reconciliation.vercel.app](https://upi-three-way-reconciliation.vercel.app) |
| **Streamlit Community Cloud** | the `dashboard/` app, pointed at a public Postgres (Neon/Supabase/Aiven) — the DSN comes from `st.secrets` |
| **a container host, continuously** | the same `docker compose` on a VPS / Fly.io / ECS, with a managed Postgres and the Iceberg warehouse on R2 or S3 — see [docs/OPERATIONS.md](docs/OPERATIONS.md) |

Flink itself cannot run on Cloudflare Workers or Vercel: it is a long-lived
JVM with stateful operators, RocksDB checkpoints and a JobManager/TaskManager
pair. Everything *around* it can.

---

## Known limitations

* **The two sinks are not atomic with each other.** `recon.match_results` and
  `recon.decision_audit` are written by two independent JDBC sinks, so a crash
  between them can leave a decision without its audit row. The next replay
  heals it (the audit key is deterministic), and the current-state table can
  never be corrupted by a stale write — see
  [decision D4](docs/DECISIONS.md).
* **The Iceberg table grows with the decision volume.** It is an append-only
  log; compaction and snapshot expiry are a maintenance job
  (`scripts/iceberg_maintenance.py`), not part of the demo loop.
* **The demo rate is 100 TPS.** The bounded join state is `rate × window`, so
  500 TPS needs more memory than an 8 GB laptop has. Raise `TPS` on a bigger
  machine — the pipeline sustains whatever the generator produces.
* **One shared TaskManager** hosts both jobs. Splitting them over two
  TaskManagers is the obvious next step for a production deployment.

---

## License

MIT — see [LICENSE](LICENSE).
