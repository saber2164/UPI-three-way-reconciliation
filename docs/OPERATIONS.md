# Operations

Running, tuning and debugging the stack.

---

## Requirements

* Docker Desktop (or Engine + Compose v2) with **≥ 8 GB** of memory allocated.
* `make`, `curl`, and `uv` (for the Python test suite) — `uv` is optional, any
  Python 3.11+ with `pytest` works.
* Ports free: `5432`, `19093`, `8085`, `8086`, `8087`, `8181`, `8503`,
  `9000`, `9001`.

```bash
cp .env.example .env     # every value is a local default
make up
make status
```

`make up` builds the images, starts the eleven services and submits the two
Flink jobs. The submit happens in a one-shot container (`flink-sql-submit`)
that waits for the Iceberg REST catalog, a registered TaskManager and the SQL
Gateway before it submits.

---

## The Make targets

| target | what it does |
|---|---|
| `make up` | build + start everything, submit both Flink jobs |
| `make down` | stop, keep the data |
| `make clean` | stop and delete the volumes (Kafka topics, Postgres, Iceberg, checkpoints) |
| `make status` | job states, decision counts, late corrections |
| `make submit` | cancel and resubmit both Flink jobs |
| `make dbt` | build the gold layer against the pinned Iceberg snapshot |
| `make test` | unit tests (no Docker needed) |
| `make integration-test` | integration tests (needs the stack and ~25 min of traffic) |
| `make verify` | both test suites |
| `make logs SERVICE=…` | tail a service's logs |
| `make ports` | the published ports |

---

## The tuning knobs

All of them are environment variables (see `.env.example`).

| variable | default | what it changes |
|---|---|---|
| `TIME_SCALE` | `60` | compresses the delays *and* the event timestamps. 60 → the 4 h SLA is 240 s; 1 → production timing |
| `TPS` | `100` | transactions per second. The bounded join state is `rate × window`, so this is the memory knob |
| `FAULT_RATE` | `0.03` | the baseline probability of injecting a fault |
| `BAD_MERCHANT` | `M0001` | the merchant that concentrates traffic and faults |
| `BAD_MERCHANT_RATE` | `0.20` | its share of the traffic |
| `BAD_MERCHANT_FAULT_RATE` | `0.35` | its share of the faults |
| `FORCE_FAULT_EVERY` | `500` | every Nth transaction gets a deterministic round-robin fault |

Changing `TIME_SCALE` or `TPS` requires a restart (`make down && make up`);
the SQL is re-rendered at submit time from `TIME_SCALE`.

**Sizing rule of thumb.** The state is proportional to
`TPS × window_seconds`. The fast job holds 4 min (scaled) and the repair job
24 min (scaled) of switch state, so the repair job dominates. On an 8 GB
laptop 100 TPS is comfortable and 500 TPS is not; the failure mode is a
TaskManager OOM, see the [failure log](FAILURE_LOG.md#13-the-taskmanager-oom-crash-looped).

---

## The resource envelope

| service | limit | note |
|---|---|---|
| flink-taskmanager | 3072 MB | the heavy one: the join state of both jobs, the Iceberg sink, five sinks |
| flink-jobmanager | 896 MB | one job graph at a time |
| flink-sql-gateway | 640 MB | the SQL front end |
| kafka | 1536 MB | **a starved broker freezes the event-time watermark** |
| iceberg-rest | 768 MB | the REST catalog |
| postgres | 512 MB | |
| minio | 512 MB | the Iceberg warehouse |
| schema-registry | 512 MB | |
| recon-generator | 512 MB | |
| truth-sink | 256 MB | |
| recon-dashboard | 512 MB | |
| dbt (one-shot) | 1024 MB | DuckDB is capped at 256 MB inside it |

Total ≈ 9.2 GB of *limits*, of which the stack normally uses ~4 GB. Docker
enforces the limit on *actual* usage, so the sum may exceed the VM size as
long as the real usage fits.

---

## Troubleshooting

| symptom | likely cause | what to do |
|---|---|---|
| the dashboard says "Waiting for first decisions" | the generator is not producing | `make logs SERVICE=recon-generator`; check the Schema Registry is up |
| the Flink jobs are missing | the submit container failed | `make logs SERVICE=flink-sql-submit`, then `make submit` |
| the pipeline froze but the jobs say RUNNING | the consumer lag grows while the decisions stop | check `docker stats` for the Kafka broker, and look for `NoResourceAvailableException` in the JobManager log (too few task slots) |
| `OutOfMemoryError` in the TaskManager | the join state does not fit | lower `TPS`, raise `taskmanager.memory.process.size`, or split the jobs over two TaskManagers |
| `Heartbeat of TaskManager ... timed out` | the TaskManager was busy restoring state | raise `heartbeat.timeout` (120 s) and use the RocksDB backend |
| the repair job never corrects anything | the two jobs share a consumer group | the group ids must differ (`recon-*-late`); check with `kafka-consumer-groups --describe --group recon-settle-late` |
| no `LATE_SETTLE` corrections yet | the most delayed settlement has not arrived | ~20 real minutes at 60×; or raise `TIME_SCALE` |
| the dbt build is OOM-killed | DuckDB is reading equality deletes | the Iceberg table must be append-only (`write.upsert.enabled = false`) |
| `Iceberg metadata file not found for table version` | a commit retired the pinned file | `dbt/run.sh` re-resolves and retries once; if it persists, re-run `make dbt` |
| the integration tests fail with a low recall | the samples are too young | the tests only count samples older than the settlement bound; wait, or raise `TIME_SCALE` |
| a port conflict | something else uses a published port | the stack uses `5432 19093 8085 8086 8087 8181 8503 9000 9001` |

Useful commands:

```bash
# what is the pipeline doing?
curl -s localhost:8087/jobs/overview | python3 -m json.tool

# how far behind is the consumer?
docker exec upi-recon-kafka-1 kafka-consumer-groups \
  --bootstrap-server localhost:9092 --describe --group recon-switch

# what did the pipeline decide?
docker exec upi-recon-postgres-1 psql -U app -d fintech -c \
  "select match_status, count(*) from recon.match_results group by 1;"

# the correction trail
docker exec upi-recon-postgres-1 psql -U app -d fintech -c \
  "select rrn, match_status, decision_ts, decided_at from recon.decision_audit
   where rrn in (select rrn from recon.decision_audit group by rrn
                 having bool_or(match_status='MISSING_SETTLEMENT')
                    and bool_or(match_status='MATCHED'))
   order by rrn, decision_ts limit 20;"

# the Flink UI's own view of the watermarks
curl -s localhost:8087/jobs/<jid>/vertices/<vid>/metrics | python3 -m json.tool
```

---

## Reset procedures

```bash
make clean && make up      # full reset: new topics, new database, new lake
make down && make up       # keep the data, restart the services
make submit                # just re-submit the Flink jobs
```

A full reset is the honest way to reproduce the measured numbers: the recall
figures are only meaningful over samples that are older than the settlement
bound, so the stack has to have been running for ~25 minutes at 60×.

---

## Upgrading the stack

* **Flink SQL changes** need `make submit` (the SQL is re-rendered and
  re-submitted). The Iceberg table survives; the DDL uses
  `CREATE TABLE IF NOT EXISTS`, so a changed table definition needs a manual
  `DROP TABLE` first.
* **Schema changes** (the Postgres tables) need `make clean && make up` — the
  init script only runs on a fresh volume.
* **Changing the state backend** invalidates the checkpoints; the jobs replay
  from the beginning once and then are back to normal.

---

## Iceberg maintenance

The lake is append-only, so it grows with the decision volume. Compaction,
snapshot expiry and orphan removal are a maintenance job, not part of the demo
loop:

```bash
# needs a Spark session with the `lake` Iceberg catalog attached
spark-submit scripts/iceberg_maintenance.py lake.recon.match_results 7
```

It bin-packs the data files, rewrites the manifests, expires the snapshots
older than N days and removes the orphaned files.
