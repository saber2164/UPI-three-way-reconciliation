# Failure log

Every failure that happened while building this pipeline, in the order it
happened, with the exact symptom, the root cause, the fix and how the fix was
verified. Most of these are Flink/DuckDB/Iceberg behaviours that are not
obvious from the documentation, so this file doubles as a reference.

---

## 1. `NoClassDefFoundError: org/apache/hadoop/hdfs/HdfsConfiguration`

**Symptom.** `CREATE CATALOG lake WITH (...)` failed on the very first submit:

```
java.lang.NoClassDefFoundError: org/apache/hadoop/hdfs/HdfsConfiguration
```

**Root cause.** The Flink image shipped `hadoop-common-3.3.4.jar` only. The
Iceberg Flink runtime calls `HadoopUtils.getHadoopConfiguration()`, which needs
`HdfsConfiguration` from `hadoop-hdfs-client`. Adding that jar then exposed the
next layer:

```
java.lang.NoClassDefFoundError: com/ctc/wstx/io/InputBootstrapper
	at org.apache.hadoop.io.WritableComparator.<init>(WritableComparator.java:128)
```

`hadoop-common` has transitive dependencies (woodstox, stax2, hadoop-auth,
guava) that the Flink distribution does not ship.

**Fix.** Ship the *fat* Hadoop client pair instead of the individual jars:

```dockerfile
curl -fsSLO .../hadoop-client-api/3.3.4/hadoop-client-api-3.3.4.jar
curl -fsSLO .../hadoop-client-runtime/3.3.4/hadoop-client-runtime-3.3.4.jar
```

`hadoop-client-api` holds the `org.apache.hadoop.*` classes and
`hadoop-client-runtime` holds the relocated third-party dependencies
(`org.apache.hadoop.shaded.*`), which is exactly what the Hadoop classes
reference. No transitive dependency hunting.

**Verified.** `CREATE CATALOG` and `CREATE TABLE lake.recon.match_results`
succeed; the Iceberg sink commits snapshots continuously.

---

## 2. `UnknownHostException: warehouse.minio`

**Symptom.** The Iceberg REST catalog answered every request with:

```
ServiceFailureException: Server error: SdkClientException: Received an
UnknownHostException ... NoSuchKey: The specified key does not exist.
```

and the requested URL was `http://minio:9000/warehouse/recon/match_results/...`
with the *bucket* in the host position: `warehouse.minio`.

**Root cause.** The catalog wrote S3 requests in virtual-hosted style, i.e.
`warehouse.minio`, because **path-style access was not enabled on the server
side**. The environment variable looked right but was not:

```yaml
CATALOG_S3_PATH__STYLE_ACCESS: "true"   # -> s3.path.style.access  (a dot!)
CATALOG_S3_PATH__STYLE__ACCESS: "true"  # -> s3.path-style-access (a dash!)
```

SmallRye maps a single `_` to `.` and a double `__` to `-`. The same mistake
affected the credentials (`CATALOG_S3_ACCESS_KEY_ID` → `s3.access.key.id`).

**Fix.** Use the double-underscore form for every hyphenated key:

```yaml
CATALOG_S3_PATH__STYLE__ACCESS: "true"
CATALOG_S3_ACCESS__KEY__ID: ${AWS_ACCESS_KEY_ID:-minio}
CATALOG_S3_SECRET__ACCESS__KEY: ${AWS_SECRET_ACCESS_KEY:-minio12345}
```

**Verified.** The catalog serves the table; the Iceberg sink writes and
commits 150+ snapshots.

---

## 3. `Interval field value 240 exceeds precision of SECOND(2) field`

**Symptom.** The rendered SQL contained `INTERVAL '240' SECOND` and Calcite
refused it:

```
org.apache.calcite.sql.validate.SqlValidatorException:
Interval field value 240 exceeds precision of SECOND(2) field
```

**Root cause.** A bare `INTERVAL 'n' SECOND` means `SECOND(2)` — Calcite's
default interval precision — whose lead field only holds `0..99`. Note that
`SECOND(4)` is *not* a way out: Flink 1.19 maps every day-time interval type to
`INTERVAL(SECOND(3))` and rejects a higher precision outright
(`DAY_INTERVAL_TYPES precision is not supported: 4`).

**Fix.** `flink/render_sql.sh` renders the largest exact unit that fits:

| seconds | literal |
|---|---|
| ≤ 99 | `INTERVAL 'n' SECOND` |
| 100 – 999 | `INTERVAL 'n' SECOND(3)` |
| 1 000 – 59 940 | `INTERVAL 'n' MINUTE` |
| 60 000 – 356 400 | `INTERVAL 'n' HOUR` |
| ≥ 357 000 | `INTERVAL 'n' DAY` |

At the default 60× scale the settlement bound becomes `INTERVAL '4' MINUTE` and
the repair bound `INTERVAL '24' MINUTE`; at 1× they become `INTERVAL '4' HOUR`
and `INTERVAL '1' DAY`.

**Verified.** The rendered SQL plans at every time scale; the fast job declares
`MISSING_SETTLEMENT` at exactly the scaled bound (measured 245 s at 60×, p95 =
max = 246 s).

---

## 4. `StreamPhysicalIntervalJoin doesn't support consuming update and delete changes`

**Symptom.** The statement set failed to plan:

```
org.apache.flink.table.api.TableException:
StreamPhysicalIntervalJoin doesn't support consuming update and delete changes
which is produced by node Deduplicate(keep=[FirstRow], key=[rrn], order=[ROWTIME])
```

**Root cause.** The original shape was `dedup → join`. A `ROW_NUMBER()` dedup is
a *retracting* (updating) operator, and a Flink interval join can only consume
an append-only input.

**Fix.** Invert the order: join the raw (append-only) Kafka streams and
deduplicate the **decision rows** afterwards.

```sql
CREATE TEMPORARY VIEW joined AS ...          -- raw interval joins
CREATE TEMPORARY VIEW decided AS
SELECT ... FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY rrn ORDER BY switch_ts ASC) AS rn
  FROM joined
) WHERE rn = 1;
```

The retry is a byte-identical copy of the switch record, so the dedup never has
to retract a row it already emitted — the stream stays insert-only in practice.

**Verified.** Both jobs plan and run; a `DUP_SWITCH` transaction produces
exactly one decision (100 % recall on that class, zero false exceptions).

---

## 5. `The query contains more than one rowtime attribute column`

**Symptom.**

```
TableException: The query contains more than one rowtime attribute column
[switch_ts, settle_ts, callback_ts] for writing into table 'pg_match_results'.
```

**Root cause.** The interval join propagates the rowtime of *both* sides, and a
sink accepts at most one event-time column.

**Fix.** Keep only `switch_ts` as a rowtime attribute and project the other
timestamps as plain values:

```sql
switch_ts,
CAST(settle_ts AS TIMESTAMP_LTZ(3)) AS settle_ts,
CAST(callback_ts AS TIMESTAMP_LTZ(3)) AS callback_ts,
```

**Verified.** The statement set plans.

---

## 6. `In UPSERT mode, partition field 'decided_date' should be included in equality fields`

**Symptom.** Creating the Iceberg table failed.

**Root cause.** Iceberg requires every partition field to be part of the
equality fields of an upsert table, and the key was `(rrn)`.

**Fix.** Make the key `(rrn, decided_date)`. (This became moot later — see
failure 17 — when the table was turned into an append-only log.)

---

## 7. `Unsupported type: TIMESTAMP_LTZ(3)`

**Symptom.** Every JDBC sink task failed on open:

```
java.lang.UnsupportedOperationException: Unsupported type:TIMESTAMP_LTZ(3)
	at AbstractJdbcRowConverter.createInternalConverter
```

**Root cause.** `flink-connector-jdbc` 3.2.0 has no row converter for
`TIMESTAMP_LTZ`.

**Fix.** Declare the JDBC sinks with `TIMESTAMP(3)` and cast explicitly in the
inserts. The Iceberg sink keeps `TIMESTAMP_LTZ(3)` — it supports it. The
container runs in UTC and the session time zone is pinned to UTC
(`SET 'table.local-time-zone' = 'UTC'`), so the two representations agree.

**Verified.** All four Postgres sinks and the Iceberg sink write rows.

---

## 8. `Failed to create directory for shared state`

**Symptom.** The job failed within a second of being submitted:

```
java.io.IOException: Failed to create directory for shared state:
file:/opt/flink/checkpoints/<jobid>/shared
```

**Root cause.** The checkpoint directory is a named volume. Docker creates the
mount point as `root:root` when the path does not exist in the image, and the
Flink process runs as the `flink` user (uid 9999) — so it could not create the
job directory.

**Fix.** Pre-create the directory in the image so a fresh volume inherits the
ownership:

```dockerfile
RUN mkdir -p $FLINK_HOME/checkpoints && chown -R flink:flink $FLINK_HOME/checkpoints
```

For an existing volume: `docker exec -u root <jobmanager> chown -R 9999:9999
/opt/flink/checkpoints`.

**Verified.** 60+ consecutive checkpoints complete, 0 failures.

---

## 9. The job was submitted to `http://0.0.0.0:8081`

**Symptom.** The SQL client reported success, but no job appeared on the
JobManager:

```
AnnotatedConnectException: Connection refused: /0.0.0.0:8081
```

**Root cause.** In gateway mode the SQL client opens a session and **sends its
own configuration to the gateway** (`OpenSessionRequestBody(sessionId,
flinkConfig.toMap())`). The gateway then uses that configuration to submit the
job. The submit container had no `rest.address`, so the default `0.0.0.0` was
used.

**Fix.** Give the submit container the cluster address explicitly:

```yaml
FLINK_PROPERTIES: |
  rest.address: flink-jobmanager
  rest.port: 8081
```

**Verified.** Both jobs appear in the Flink UI within seconds of the submit.

---

## 10. The repair job never corrected anything

**Symptom.** `LATE_SETTLE` transactions stayed `MISSING_SETTLEMENT` forever.
The repair job was RUNNING, the topics had data, the lag was zero — and no
correction was ever produced.

**Root cause.** Both jobs read the same three topics with the **same Kafka
consumer group ids**. Two Flink jobs in one consumer group are two members of
that group, and the group coordinator hands each partition to exactly one
member. The fast job won all three partitions; the repair job got nothing and
idled forever.

**Fix.** Give the repair job its own copies of the sources with their own group
ids (`recon-switch-late`, `recon-settle-late`, `recon-callback-late`). The
three topics are declared in `30_late_correction.sql` for exactly this reason.

**Verified.** 4 000+ LATE_SETTLE transactions were corrected, each with
`MISSING_SETTLEMENT → MATCHED` and a strictly increasing `decision_ts`.

---

## 11. The oracle sink crash-looped

**Symptom.** `truth-sink` died and restarted in a loop:

```
psycopg2.errors.CardinalityViolation: ON CONFLICT DO UPDATE command cannot
affect row a second time
```

**Root cause.** A batch can contain the same RRN twice (a producer retry, or a
message replayed after a restart). Postgres refuses an `ON CONFLICT DO UPDATE`
that touches the same key twice, the whole batch was rolled back, and the
offsets were never committed — so the same batch came back forever.

**Fix.** Collapse the batch by RRN before the upsert and fall back to
row-by-row inserts if the batch still fails, so a single bad row can never
kill the sink:

```python
deduped = {}
for row in rows:
    deduped[row[0]] = row
rows = list(deduped.values())
```

**Verified.** 0 `CardinalityViolation` lines; the oracle tracks the generator
1:1 with a lag of ~10 records.

---

## 12. The submit container hung for 20 minutes

**Symptom.** `make submit` never returned; the container stayed up long after
the SQL client had exited.

**Root cause.** Two unbounded waits: the `curl` probes had no timeout, and the
SQL client could wait forever on a gateway operation that never completed.

**Fix.** A `curl` wrapper with `--connect-timeout 5 --max-time 30` and a
`timeout --signal=TERM 900` around the SQL client.

**Verified.** The submit either succeeds or fails within a bounded time.

---

## 13. The TaskManager OOM-crash-looped

**Symptom.** The pipeline silently stopped. The jobs showed RUNNING, no
decisions were produced, and the consumer lag grew without bound.

```
java.lang.OutOfMemoryError: Java heap space
Exception: java.lang.OutOfMemoryError thrown from the UncaughtExceptionHandler
in thread "jdbc-upsert-output-format-thread-1"
FATAL: Thread 'jdbc-upsert-output-format-thread-1' produced an uncaught
exception. Stopping the process...
```

followed by, every ~60 s:

```
Heartbeat of TaskManager with id 172.20.0.10:37009 timed out
NoResourceAvailableException: Could not acquire the minimum required resources
```

**Root cause.** Two compounding problems.

1. The **heap** state backend keeps the interval-join state in the JVM heap.
   At 500 TPS the two jobs held ~326 MB of join state in a ~1 GB heap, and a
   restore deserialised ~50 MB per task — long enough for the TaskManager to
   miss its heartbeat, which failed the jobs, which restored again.
2. The **Kafka broker was starved** at 768 MB. A broker that cannot serve
   fetches stalls the consumers, the event-time watermark freezes, and the
   interval join keeps every buffered row: the state then grows without bound
   and the checkpoint with it.

**Fix.**

| Change | Reason |
|---|---|
| `state.backend.type: rocksdb`, `state.backend.incremental: true` | the join state lives on disk; checkpoints are incremental (5–15 MB instead of 50 MB) and a restore does not block the heartbeat |
| `heartbeat.timeout: 120000`, `heartbeat.interval: 10000` | a laptop under load can miss a 50 s heartbeat while restoring |
| Kafka heap 1 GB, container limit 1536 MB | a starved broker freezes the watermark |
| demo rate 100 TPS (from 500) | the bounded state is `rate × window` |
| `taskmanager.numberOfTaskSlots: 4` | both jobs need a slot group each (see failure 14) |

**Verified.** 4+ hours with `RestartCount=0`, 0 `OutOfMemoryError` lines,
checkpoints completing in 80–350 ms, and the consumer lag stable at ~1–2 k
messages.

---

## 14. `NoResourceAvailableException` with 3 task slots

**Symptom.** The jobs restarted every few seconds and the TaskManager showed
`Registered TMs: 1, registered slots: 0, free slots: 0`.

**Root cause.** The two jobs are chained into several slot groups each. With
only three slots the scheduler could not place every group, so it kept
restarting the tasks that were already running.

**Fix.** Four slots (one group per job plus headroom).

**Verified.** Both jobs stable, `slots-available: 2` at idle.

---

## 15. The dbt build was OOM-killed

**Symptom.** `make dbt` died with exit 137 while materialising the staging
model — the container was at its memory limit with 9 MB of Parquet.

**Root cause.** DuckDB's `iceberg_scan` materialises the **equality-delete
index** for every data file. The Iceberg table was written with
`write.upsert.enabled = true`, so every commit carried equality deletes, and
DuckDB needed more than 2 GB to read 9 MB of data — more than fits next to the
streaming stack on an 8 GB laptop.

**Fix.** Turn the Iceberg sink into an **append-only decision log**
(`write.upsert.enabled = false`). The same scan then costs 0.3 s and 110 MB.
The "latest decision per RRN" rule that the deletes would have encoded moves
into the dbt staging model, which already deduplicates.

Reading the data files directly (`read_parquet`) was measured as the
alternative: 0.3 s and 110 MB as well, but it loses the Iceberg snapshot
semantics, so the append-only log was preferred.

**Verified.** The dbt build completes in ~4 s with a 1 GB container.

---

## 16. `Iceberg metadata file not found for table version ...`

**Symptom.** The dbt build failed with:

```
HTTP Error: HTTP GET error reading
'.../metadata/00045-<uuid>.metadata.json/metadata/snap-<id>.avro' (404)
```

**Root cause.** Two problems in one.

1. DuckDB 1.5 resolves the manifest list **relative to the path you hand it**,
   not relative to the metadata file. Given the metadata.json path it looked
   for `<metadata.json>/metadata/snap-....avro`.
2. The Iceberg REST catalog names its metadata files `<counter>-<uuid>.metadata.json`,
   while DuckDB derives the file name from a *version number*:
   `v{version}.metadata.json` or `{version}.metadata.json`.

**Fix.** Pass the table root plus an explicit version and a matching name
format:

```sql
iceberg_scan('s3://warehouse/recon/match_results',
             version='00045-3d5e1f78-...',
             version_name_format='%s%s.metadata.json',
             allow_moved_paths=true)
```

The macro derives the root and the version from the resolved metadata
location, and `dbt/run.sh` re-resolves and retries once if a commit retires
the pinned file between the resolve and the scan.

**Verified.** The gold layer builds against a pinned snapshot.

---

## 17. The audit trail was inflated by every replay

**Symptom.** After a job restart, `recon.decision_audit` had 360 657 rows for
165 855 RRNs. 64 969 RRNs had more than one row, and only 114 of those were
genuine corrections — the rest were the same decision re-emitted.

**Root cause.** The audit was keyed `(rrn, decided_at)` and `decided_at` was
`CURRENT_TIMESTAMP`, the wall clock. A replay re-decides every transaction, so
every replay invented a new "history" row.

**Fix.** Key the audit by the **event time the decision is anchored to**:

| job | `decision_ts` | meaning |
|---|---|---|
| fast | `switch_ts` | the decision is made when the switch is seen |
| repair | `settle_ts` | the correction is made when the late settlement arrives |

```sql
PRIMARY KEY (rrn, decision_ts) NOT ENFORCED
```

The key is deterministic, so a replay is idempotent, and the two jobs get
different keys for the same RRN — which is exactly the history we want to keep.

**Verified.** 99.9 % of RRNs have exactly one audit row; the rest are the
corrections.

---

## 18. A correction could be undone by a replay

**Symptom.** 23 RRNs showed `MATCHED → MISSING_SETTLEMENT`: the repair job had
corrected them and the fast job's replay had put the exception back.

**Root cause.** `recon.match_results` is upserted by RRN, and last-write-wins
does not know which writer is more correct.

**Fix.** Make the serving table **monotonic in decision time** with a trigger:

```sql
CREATE OR REPLACE FUNCTION recon.reject_stale_decision() RETURNS trigger AS $$
BEGIN
  IF OLD.decision_ts IS NOT NULL
     AND NEW.decision_ts IS NOT NULL
     AND NEW.decision_ts < OLD.decision_ts THEN
    RETURN OLD;                     -- keep the newer decision
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_match_results_monotonic
  BEFORE UPDATE ON recon.match_results
  FOR EACH ROW EXECUTE FUNCTION recon.reject_stale_decision();
```

The fast job's decision is anchored at `switch_ts`, the repair job's at
`settle_ts` (which is later), so the correction always wins and a stale
replay is rejected.

**Verified.** 0 regressions, checked two independent ways.

---

## 19. The generator could emit the same RRN twice

**Symptom.** Exactly one transaction in 270 358 was classified
`AMOUNT_MISMATCH` without an injected fault. The Kafka log showed two
settlements under one RRN, with different UTRs, different merchants and
different amounts.

**Root cause.** The RRN was drawn at random:

```python
rrn = f"{rng.randint(10**11, 10**12 - 1)}"
```

The RRN is the natural key of the whole pipeline, so a collision merges two
unrelated payments into one decision. At 270 k transactions the birthday
probability is ~4 %.

**Fix.** Allocate RRNs from a random prefix plus a monotonic counter — unique
per process, still 12 digits, O(1) memory:

```python
_RRN_PREFIX = random.SystemRandom().randint(1000, 9999)
_rrn_counter = itertools.count(1)

def new_rrn() -> str:
    return f"{_RRN_PREFIX}{next(_rrn_counter) % 100_000_000:08d}"
```

**Verified.** 50 000 RRNs are unique; a regression test asserts it.

---

## 20. A generator restart silently dropped in-flight legs

**Symptom.** After restarting the generator, ~180 transactions were reported as
`MISSING_SETTLEMENT` although the oracle said `MATCHED` — and they never
recovered.

**Root cause.** On shutdown the generator flushed only the legs that were
*already due*:

```python
flush_due(utcnow())     # everything due later was dropped
```

A settlement due in two minutes still belongs to a transaction the pipeline
has already seen. Dropping it makes the reconciler report a missing
settlement for a payment that was actually settled.

**Fix.** Drain the whole queue on shutdown.

**Verified.** The `DUP_SWITCH` integration test (no false exception) passes
again after a generator restart.

---

## 21. The integration test used the wrong age filter

**Symptom.** The recall tests failed with 11 % recall on a healthy pipeline.

**Root cause.** The age filter was derived from the *leg delay* (45 min in
production time) divided by `TIME_SCALE`, but it forgot the **settlement
bound**: a decision cannot exist before the watermark has passed
`switch_ts + 4 h`. The filter also used the leg delay where it needed
`max(leg delay, settlement bound)`.

**Fix.**

```python
SETTLE_BOUND_REAL_S = 4 * 3600

def aged_minutes(real_seconds: float) -> int:
    need = max(float(real_seconds), SETTLE_BOUND_REAL_S)
    return max(5, int(math.ceil(need / max(TIME_SCALE, 1.0) / 60.0)) + 5)
```

At 60× that is ~9 minutes for the ordinary fault classes and ~25 minutes for
the late-settlement repair; before that the late test skips itself.

**Verified.** 8/8 integration tests pass on a healthy stack.

---

## 22. `TUMBLE(TABLE decided, DESCRIPTOR(proc_time), ...)` over a view

**Symptom.** *Not* a failure — a suspected one. A static review of the plan
warned that `DESCRIPTOR(proc_time)` over a view column is not a
time-indicator type and would fail with
`The window function requires the timecol is a time attribute type`.

**Reality.** The `PROCTIME()` call survives the projection of the view, the
windowing TVF plans, and both the KPI table and the alert table are populated
(6 994 KPI rows, 81 alerts, with the alert firing only for the injected bad
merchant).

**Decision.** Keep the current form; it works. Recorded here because the
warning is easy to rediscover.

---

## 23. `EXECUTE STATEMENT SET BEGIN ... END` and the SQL client

**Symptom.** *Not* a failure either. The same review claimed the SQL client's
line-oriented file mode splits the statement set at the first inner `;` and
the gateway therefore sees a truncated statement.

**Reality.** The gateway logs a burst of `SqlParserEOFException` during the
submit (it tries the fragments) and then executes the statement set
correctly: both jobs are submitted with all five sinks, and every sink
receives rows. The error burst is confined to the submit window and is
harmless.

**Decision.** Keep the documented `EXECUTE STATEMENT SET` form. Recorded so
the next reader does not "fix" a working pipeline.

---

## 24. The decision latency looked like 15 minutes

**Symptom.** A latency analysis showed `decided_at - switch_ts` with a median
of 920 s and the decisions arriving in ~2 000-row bursts every 16 minutes.

**Root cause.** The host was memory-starved (three unrelated containers
without limits plus this stack), which slowed the *generator* and left the
pipeline replaying a backlog in bursts. The design is unaffected.

**Reality.** On an unpressured host the fast job declares `MISSING_SETTLEMENT`
at a deterministic 245 s (the scaled 4 h bound) with p95 = max = 246 s, and the
matched decisions are written 0.2–1 s after the settlement arrives.

**Lesson.** Measure the latency on an idle machine, and compare it with the
*design* bound rather than with a wall-clock intuition.

---

## 25. `OutOfMemoryError` in the JDBC upsert thread

**Symptom.** The TM died with

```
OutOfMemoryError: Java heap space
in thread "jdbc-upsert-output-format-thread-1"
```

at 500 TPS.

**Root cause.** The heap state backend plus five sinks in one TM with a ~1 GB
heap. See failure 13 for the full cascade.

**Fix.** RocksDB + a larger heap + a lower demo rate.

---

## 26. `SqlExecutionException: Failed to fetchResults` during the submit

**Symptom.** Hundreds of errors in the SQL gateway log at the moment of the
submit.

**Reality.** The client asks for results while the gateway is still parsing
and executing the previous statement. The errors are timestamped inside the
submit window only; both jobs start and run normally.

**Decision.** Not fixed. Documented as benign submit-window noise.

---

## 27. `pull access denied for upi-recon-flink`

**Symptom.** The GitHub Actions simulation failed in its first step:

```
Error pull access denied for upi-recon-flink, repository does not exist
or may require 'docker login': denied: requested access to the resource is denied
```

**Root cause.** Only `flink-jobmanager` declared a `build:` section; the
TaskManager, the SQL Gateway and the submit container only declared `image:`.
Compose then tried to **pull** the locally built image from the registry for
those services instead of building it. On a laptop the image already existed
in the local cache, so it worked; on a fresh runner it failed.

**Fix.** Every service that uses a locally built image declares the same
`build:` section (the build is cached, so the extra declarations cost
nothing).

**Verified.** The stack now starts on a fresh GitHub runner.

---

## 28. `minio-init` failed with exit 1 and took the stack with it

**Symptom.** The same workflow failed in the first step again:

```
Container upi-recon-minio-init-1  service "minio-init" didn't complete
successfully: exit 1
```

**Root cause.** The one-shot init creates the two buckets with a single
`mc alias set && mc mb && mc mb` chain. MinIO's health check can pass before
its S3 API answers, and `mc mb` on an existing bucket also exits 1 — either
way the init dies, and because `iceberg-rest` and the Flink JobManager both
declare `condition: service_completed_successfully`, the whole stack stops
before it starts.

**Fix.** The init retries the alias until MinIO answers and uses
`--ignore-existing`, and the workflow prints the init's log if the stack does
not come up.

**Verified.** The stack starts on the runner and the simulation runs.

---

## 29. The Vercel edge runtime cannot reach github.com

**Symptom.** The dashboard's API returned a 504 on every request, with the
invocation burning its full 30 s budget:

```
FUNCTION_INVOCATION_TIMEOUT
```

**Root cause.** The function fetched the published run from
`raw.githubusercontent.com` (and then from `api.github.com`). Both requests
*hang* from Vercel's edge runtime rather than failing, and the request
timeout is not honoured either.

**Fix.** Don't fetch at runtime. The build embeds `runs/latest.json` and
`runs/history.json` in the site and the dashboard page reads them statically —
no API, no database, no CDN cache in the way, and no dependency on GitHub
being reachable from the edge. The API route was removed.

**Verified.** The dashboard renders the published run on the edge.

---

## Summary of the classes of failure

| Class | Failures | Root cause pattern |
|---|---|---|
| Image / dependency packaging | 1, 2 | a jar is never a jar: transitive dependencies, S3 style, MinIO image availability |
| Flink SQL semantics | 3, 4, 5, 6, 7 | interval precision, retracting inputs, rowtime cardinality, Iceberg equality-field rules, connector type support |
| Deployment / wiring | 8, 9, 10, 12 | volume ownership, client configuration, consumer groups, unbounded waits |
| Capacity | 13, 14, 15, 25 | heap state, slot starvation, a starved broker, DuckDB's delete index |
| Correctness under replay | 17, 18, 19, 20 | wall-clock keys, last-write-wins, random natural keys, lost in-flight data |
| Measurement | 21, 24 | the age filter must follow the SLA, and the machine must be idle |
| Cloud deployment | 26, 27, 28, 29 | a locally built image must be declared as a build, a one-shot init must be idempotent and retried, and the edge must not depend on a runtime fetch |
