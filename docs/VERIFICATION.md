# Verification

What the project asserts, and what it actually measured on a clean run.

---

## The test suites

```bash
make test             # 33 unit tests, no Docker required
make integration-test # 8 tests against the running stack
make dbt              # 23 dbt nodes (4 models + 4 marts + 15 tests)
```

### Unit tests (`tests/`)

| file | what it pins down |
|---|---|
| `test_model.py` | the fault semantics: which status each fault class must produce, the initial-vs-final distinction for `LATE_SETTLE`, the time-scaling behaviour, the fault scheduler's coverage, the merchant concentration, and that the RRNs are unique |
| `test_decision_logic.py` | the decision table itself, mirrored in Python from the SQL `CASE`: the precedence of the settlement leg, the `MISSING_CALLBACK` case, the amount comparison, and the null-amount edges |

The model is deliberately dependency-free — no Kafka, no database, no Docker —
so the interesting part (the decision table and the fault semantics) is
testable on a laptop.

### Integration tests (`tests/test_fault_recall.py`)

Marked `integration`; they skip cleanly when Postgres is unreachable, so
`pytest -m "not integration"` stays a pure unit suite.

| test | assertion |
|---|---|
| `test_final_recall[AMOUNT_DRIFT]` | ≥ 95 % of the aged `AMOUNT_DRIFT` samples are `AMOUNT_MISMATCH` |
| `test_final_recall[MISSING_CALLBACK]` | ≥ 95 % are `MISSING_CALLBACK` |
| `test_final_recall[MISSING_SETTLE]` | ≥ 95 % are `MISSING_SETTLEMENT` |
| `test_final_recall[DUP_SWITCH]` | ≥ 95 % are `MATCHED` — a retry must not change the verdict |
| `test_final_recall[OUT_OF_ORDER]` | ≥ 95 % are `MATCHED` — a skewed clock must not change the verdict |
| `test_late_settlement_initially_missing_then_corrected` | the late settlement first appears as `MISSING_SETTLEMENT` and is later corrected to `MATCHED` (≥ 95 %) |
| `test_duplicate_switch_does_not_create_extra_exception` | no `DUP_SWITCH` transaction ever becomes an exception |
| `test_control_total_identity_holds` | no row contradicts its own status (`MISSING_SETTLEMENT` with a settlement, `AMOUNT_MISMATCH` with equal amounts) |

**The age filter is part of the test.** A decision cannot exist before the
settlement bound has passed, so the tests only count samples that are older
than `max(leg delay, settlement bound)`:

```python
SETTLE_BOUND_REAL_S = 4 * 3600      # the fast job's 4 h bound

def aged_minutes(real_seconds: float) -> int:
    need = max(float(real_seconds), SETTLE_BOUND_REAL_S)
    return max(5, int(math.ceil(need / max(TIME_SCALE, 1.0) / 60.0)) + 5)
```

At 60× that is ~9 minutes for the ordinary classes and ~25 minutes for the
late-settlement repair. Before that the late test skips itself rather than
failing.

### dbt tests

| test | assertion |
|---|---|
| `assert_amounts_balance` | `gross = reconciled + mismatch + outstanding + missing_callback` for every day and merchant |
| `assert_no_duplicate_rrn` | the staging model's deduplication holds — one row per RRN |
| `assert_daily_recon_unique` | one row per (day, merchant) |
| `assert_match_rate_range` | the match rate is a sane percentage |

Plus the generic tests (not null, unique, accepted values) on the staging and
mart models.

---

## The measured results

From a clean run of the stack (reset, then 30+ minutes of traffic at 100 TPS
and `TIME_SCALE=60`):

### Detection quality

| fault class | samples (> 12 min old) | final recall |
|---|---|---|
| `AMOUNT_DRIFT` | 1 279 | **100.00 %** |
| `DUP_SWITCH` | 1 285 | **100.00 %** |
| `LATE_SETTLE` | 1 286 | **100.00 %** |
| `MISSING_CALLBACK` | 1 313 | **100.00 %** |
| `MISSING_SETTLE` | 1 232 | **100.00 %** |
| `OUT_OF_ORDER` | 1 286 | **100.00 %** |

* false positives (an exception without an injected fault): **0**
* status regressions (`MATCHED → MISSING_SETTLEMENT`): **0**
* misclassified statuses (neither the expected final nor the expected
  initial): **0**

### The correction trail

1 286 LATE_SETTLE transactions were corrected. Each one has two audit rows
with a strictly increasing `decision_ts`:

| rrn | match_status | decision_ts | decided_at |
|---|---|---|---|
| 100 226 094 266 | `MISSING_SETTLEMENT` | 2026-10-20 22:14:07.212 | 2026-10-20 22:18:12.841 |
| 100 226 094 266 | `MATCHED` | 2026-10-20 22:33:50.792 | 2026-10-20 22:35:58.132 |

The first row is anchored at the switch time (the fast job's 4 h bound
expired), the second at the settlement time (the repair job caught it).

### The finance control total

| bucket | rupees |
|---|---|
| gross | 185 811 140.93 |
| reconciled (`MATCHED`) | |
| mismatch (`AMOUNT_MISMATCH`) | |
| outstanding (`MISSING_SETTLEMENT`) | |
| missing callback (`MISSING_CALLBACK`) | |
| **difference** | **0.00** |

The identity holds to the paisa, in both rupees and in transaction counts.

### The pipeline

| property | measured |
|---|---|
| decisions | 85 219 |
| audit rows | 86 505 (= decisions + the corrections) |
| throughput | ~100 decisions/s, matching the generator |
| consumer lag | 1–2.5 k messages, stable |
| checkpoint | 5–15 MB, 80–350 ms, 0 failures |
| TaskManager | 0 restarts, 0 `OutOfMemoryError`, ~54 % of its limit |
| decision latency | 245 s for `MISSING_SETTLEMENT` (exactly the scaled 4 h bound), 0.2–1 s for a matched row after its settlement arrives |
| late corrections | 5–20 min after the switch, as designed |

### The suites

| suite | result |
|---|---|
| unit | 33 passed |
| integration | 8 passed |
| dbt | 23/23 nodes, 0 errors |

---

## How to reproduce the numbers

```bash
make clean && make up       # a clean stack
# wait ~30 minutes: the late-settlement repair needs the most delayed
# settlement (20 real minutes at 60x) and the recall tests need samples
# older than the settlement bound.
make verify
make dbt
make status
```

The numbers move with the machine: an idle host sustains the generator's
rate, a memory-starved host throttles the *generator* and the pipeline
follows it down. The correctness figures (recall, false positives,
regressions, the control total) do not move — they are properties of the
logic, not of the load.
