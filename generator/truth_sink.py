"""
Consume the JSON ground-truth side channel and upsert it into Postgres.

The streaming job's correctness is measured against this table, so it must be
kept out of the Flink path: if the reconciler is wrong, the oracle has to still
be right.
"""
from __future__ import annotations

import json
import os
import signal
import sys
import time
from datetime import datetime, timezone

import psycopg2
from confluent_kafka import Consumer
from psycopg2.extras import execute_values

BOOT = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
DSN = os.getenv("POSTGRES_DSN", "postgresql://app:app@postgres:5432/fintech")
TOPIC = os.getenv("TRUTH_TOPIC", "recon.ground_truth")
GROUP = os.getenv("TRUTH_GROUP", "truth-sink-standalone")
BATCH = int(os.getenv("TRUTH_BATCH", "500"))

_running = True


def _stop(signum, _frame):
    global _running
    print(f"received signal {signum}; committing and stopping", flush=True)
    _running = False


def connect_with_retry(attempts: int = 60, delay: float = 2.0):
    last = None
    for i in range(attempts):
        try:
            return psycopg2.connect(DSN)
        except Exception as exc:  # pragma: no cover - startup concern
            last = exc
            print(f"postgres not ready ({i + 1}/{attempts}): {exc}", flush=True)
            time.sleep(delay)
    raise RuntimeError(f"could not connect to Postgres: {last}")


def row_from_message(raw: bytes):
    p = json.loads(raw)
    injected = p.get("injected_at")
    if injected:
        try:
            injected = datetime.fromisoformat(injected.replace("Z", "+00:00"))
        except ValueError:
            injected = datetime.now(timezone.utc)
    else:
        injected = datetime.now(timezone.utc)
    return (
        p["rrn"],
        p.get("fault"),
        p.get("merchant_id"),
        p.get("expected_status"),
        p.get("expected_initial", p.get("expected_status")),
        injected,
    )


def main() -> int:
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    consumer = Consumer({
        "bootstrap.servers": BOOT,
        "group.id": GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "session.timeout.ms": 10000,
    })
    consumer.subscribe([TOPIC])

    conn = connect_with_retry()
    conn.autocommit = False
    sql = """
        INSERT INTO recon.ground_truth
            (rrn, fault, merchant_id, expected_status, expected_initial, injected_at)
        VALUES %s
        ON CONFLICT (rrn) DO UPDATE SET
            fault = EXCLUDED.fault,
            merchant_id = EXCLUDED.merchant_id,
            expected_status = EXCLUDED.expected_status,
            expected_initial = EXCLUDED.expected_initial,
            injected_at = EXCLUDED.injected_at
    """

    total = 0
    print(f"truth sink listening on {TOPIC} as {GROUP}", flush=True)
    try:
        while _running:
            messages = consumer.consume(num_messages=BATCH, timeout=2.0)
            rows = []
            valid = []
            for msg in messages:
                if msg.error():
                    # The topic may not exist yet; the generator creates it on
                    # its first produce.  Do not commit: there is no offset.
                    print(f"kafka error: {msg.error()}", file=sys.stderr, flush=True)
                    continue
                valid.append(msg)
                try:
                    rows.append(row_from_message(msg.value()))
                except Exception as exc:
                    print(f"bad truth payload: {exc}", file=sys.stderr, flush=True)

            if rows:
                # The same RRN can appear twice in one batch (a producer retry,
                # or a message replayed after a restart).  Postgres refuses an
                # "ON CONFLICT DO UPDATE" that touches the same key twice
                # ("cannot affect row a second time"), which killed the sink in
                # a crash loop and left the oracle stale.  Collapse the batch
                # first, keeping the newest row per RRN.
                deduped = {}
                for row in rows:
                    deduped[row[0]] = row
                rows = list(deduped.values())
                with conn.cursor() as cur:
                    try:
                        execute_values(cur, sql, rows)
                    except Exception as exc:  # noqa: BLE001 - keep the sink alive
                        conn.rollback()
                        print(f"batch upsert failed, falling back to row-by-row: {exc}",
                              file=sys.stderr, flush=True)
                        with conn.cursor() as cur:
                            for row in rows:
                                try:
                                    execute_values(cur, sql, [row])
                                except Exception as row_exc:  # noqa: BLE001
                                    conn.rollback()
                                    print(f"row upsert failed ({row[0]}): {row_exc}",
                                          file=sys.stderr, flush=True)
                conn.commit()
                total += len(rows)
                print(f"upserted {len(rows)} truth rows (total {total})", flush=True)

            if valid:
                try:
                    consumer.commit(asynchronous=False)
                except Exception as exc:  # noqa: BLE001 - rebalance/offset race
                    print(f"offset commit failed (will retry): {exc}", file=sys.stderr, flush=True)
            elif not messages:
                continue
            else:
                # Unknown topic/partition: back off instead of hot-looping.
                time.sleep(1)
    finally:
        try:
            conn.close()
        finally:
            consumer.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
