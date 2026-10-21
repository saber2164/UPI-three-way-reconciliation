"""
Three-leg UPI transaction simulator with injected real-world faults.

Happy path: switch SUCCESS -> merchant callback ACK within seconds -> bank
settlement row minutes-to-hours later.

Injected faults (``FAULT_RATE`` total, plus a deterministic round-robin so every
class appears even in a short demo):

  DUP_SWITCH        same RRN emitted twice by the switch (network retry)
  AMOUNT_DRIFT      settlement amount differs by a few paise/rupees
  MISSING_SETTLE    settlement never arrives
  LATE_SETTLE       settlement arrives after the fast job's bounded state
  MISSING_CALLBACK  merchant webhook never arrives
  OUT_OF_ORDER      callback event time precedes the switch event (clock skew)

``TIME_SCALE`` compresses wall-clock delays.  With the default 60, a 4-hour
settlement SLA becomes 4 minutes, and a 20-hour late settlement becomes 20
minutes.  Event timestamps are scaled too, so Flink's event-time joins and the
demo both observe the same ordering.
"""
from __future__ import annotations

import heapq
import json
import os
import random
import signal
import sys
import time
import urllib.error
import urllib.request
from collections import Counter
from datetime import datetime, timezone

from confluent_kafka import Producer, SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

import schemas
from model import (
    DEFAULT_BAD_MERCHANT,
    FaultScheduler,
    build_transaction,
    choose_merchant,
    utcnow,
)

BOOT = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
SR = os.getenv("SCHEMA_REGISTRY", "http://schema-registry:8081")
TPS = int(os.getenv("TPS", "500"))
FAULT_RATE = float(os.getenv("FAULT_RATE", "0.03"))
TIME_SCALE = float(os.getenv("TIME_SCALE", "60"))
BAD_MERCHANT = os.getenv("BAD_MERCHANT", DEFAULT_BAD_MERCHANT)
BAD_MERCHANT_RATE = float(os.getenv("BAD_MERCHANT_RATE", "0.20"))
BAD_MERCHANT_FAULT_RATE = float(os.getenv("BAD_MERCHANT_FAULT_RATE", "0.35"))
FORCE_FAULT_EVERY = int(os.getenv("FORCE_FAULT_EVERY", "500"))
SEED = os.getenv("SEED")

TRUTH_TOPIC = "recon.ground_truth"

_running = True


def _stop(signum, _frame):
    global _running
    print(f"received signal {signum}; flushing producers and stopping", flush=True)
    _running = False


def _delivery(err, msg):
    if err is not None:
        print(f"delivery failed for {msg.topic()}: {err}", file=sys.stderr, flush=True)


def wait_for_schema_registry(attempts: int = 90, delay: float = 2.0) -> None:
    """Docker healthchecks can race the HTTP readiness of Schema Registry."""
    url = SR.rstrip("/") + "/subjects"
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(url, timeout=5) as response:
                if response.status == 200:
                    return
        except (urllib.error.URLError, OSError) as exc:
            if attempt % 5 == 0:
                print(f"waiting for Schema Registry at {url} ({attempt + 1}/{attempts}): {exc}", flush=True)
        time.sleep(delay)
    raise RuntimeError(f"Schema Registry did not become ready at {url}")


def main() -> int:
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    rng = random.Random(int(SEED)) if SEED else random.Random()
    scheduler = FaultScheduler(FAULT_RATE, FORCE_FAULT_EVERY, rng)

    try:
        wait_for_schema_registry()
        sr_client = SchemaRegistryClient({"url": SR})
        value_serializers = {
            topic: AvroSerializer(sr_client, json.dumps(schema))
            for topic, schema in schemas.SCHEMAS.items()
        }
    except Exception as exc:  # pragma: no cover - startup/runtime concern
        print(f"cannot initialise Schema Registry client: {exc}", file=sys.stderr)
        return 1

    producers = {
        topic: SerializingProducer({
            "bootstrap.servers": BOOT,
            "key.serializer": StringSerializer(),
            "value.serializer": value_serializers[topic],
            "linger.ms": 10,
            "batch.size": 65536,
            "compression.type": "snappy",
            "enable.idempotence": True,
            "acks": "all",
            "retries": 5,
            "retry.backoff.ms": 200,
        })
        for topic in schemas.SCHEMAS
    }
    p_truth = Producer({
        "bootstrap.servers": BOOT,
        "linger.ms": 20,
        "batch.size": 32768,
        "acks": "1",
        "retries": 5,
    })

    pending: list[tuple[datetime, int, object]] = []
    seq = 0
    produced = Counter()
    faults = Counter()
    last_report = time.monotonic()

    def produce(outbound) -> None:
        producers[outbound.topic].produce(
            outbound.topic, key=outbound.key, value=outbound.value,
            on_delivery=_delivery,
        )
        produced[outbound.topic] += 1

    def flush_due(now: datetime) -> None:
        nonlocal seq
        while pending and pending[0][0] <= now:
            _, _, outbound = heapq.heappop(pending)
            produce(outbound)

    print(
        f"generating {TPS} tps | fault_rate={FAULT_RATE} | time_scale={TIME_SCALE}x | "
        f"bad_merchant={BAD_MERCHANT} ({BAD_MERCHANT_RATE:.0%} traffic, "
        f"{BAD_MERCHANT_FAULT_RATE:.0%} faults)",
        flush=True,
    )

    try:
        while _running:
            started = time.monotonic()
            now = utcnow()
            for _ in range(TPS):
                merchant = choose_merchant(rng, BAD_MERCHANT, BAD_MERCHANT_RATE)
                rate = BAD_MERCHANT_FAULT_RATE if merchant == BAD_MERCHANT else FAULT_RATE
                fault = scheduler.next(rate)
                txn = build_transaction(
                    now, rng, time_scale=TIME_SCALE, fault=fault,
                    merchant_id=merchant, bad_merchant=BAD_MERCHANT,
                    bad_merchant_rate=BAD_MERCHANT_RATE,
                )
                for outbound in txn.outbound:
                    seq += 1
                    heapq.heappush(pending, (outbound.due_at, seq, outbound))
                if txn.truth is not None:
                    p_truth.produce(
                        TRUTH_TOPIC,
                        key=txn.rrn.encode(),
                        value=json.dumps(txn.truth).encode(),
                        on_delivery=_delivery,
                    )
                    faults[fault] += 1

            flush_due(now)
            for producer in producers.values():
                producer.poll(0)
            p_truth.poll(0)

            if time.monotonic() - last_report >= 10:
                print(
                    f"produced={dict(produced)} pending={len(pending)} faults={dict(faults)}",
                    flush=True,
                )
                last_report = time.monotonic()

            elapsed = time.monotonic() - started
            time.sleep(max(0.0, 1.0 - elapsed))
    finally:
        # Drain the *whole* queue, not just the parts that are already due.  A
        # settlement that is due in two minutes still belongs to a transaction
        # the pipeline has already seen: dropping it makes the reconciler report
        # a MISSING_SETTLEMENT for a payment that was actually settled, and the
        # ground truth (which says MATCHED) then contradicts the pipeline.
        while pending:
            _, _, outbound = heapq.heappop(pending)
            produce(outbound)
        for producer in producers.values():
            producer.flush(10)
        p_truth.flush(10)
        print(f"stopped; totals produced={dict(produced)} faults={dict(faults)}", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
