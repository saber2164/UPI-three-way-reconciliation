"""
Pure-Python model for the three-leg UPI transaction simulator.

Nothing in this module imports Kafka, a database driver, or Docker.  That is
deliberate: the interesting part of the project is the *decision table* and the
*fault semantics*, so they can be unit-tested on a laptop without starting the
platform.

Amounts are integer paise everywhere.  A single float rupee in a reconciliation
pipeline is how you end up explaining a ₹0.01 break to an auditor.
"""
from __future__ import annotations

import itertools
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import random
import uuid
from typing import Optional

FAULTS = (
    "DUP_SWITCH",
    "AMOUNT_DRIFT",
    "MISSING_SETTLE",
    "LATE_SETTLE",
    "MISSING_CALLBACK",
    "OUT_OF_ORDER",
)

# Final status a correct pipeline should converge on.
EXPECTED_FINAL = {
    "NONE": "MATCHED",
    "DUP_SWITCH": "MATCHED",
    "AMOUNT_DRIFT": "AMOUNT_MISMATCH",
    "MISSING_SETTLE": "MISSING_SETTLEMENT",
    "LATE_SETTLE": "MATCHED",          # late correction job updates the row
    "MISSING_CALLBACK": "MISSING_CALLBACK",
    "OUT_OF_ORDER": "MATCHED",
}

# Status the *fast* 4-hour (time-scaled) job should emit first.  Only LATE_SETTLE
# differs: the first pass cannot see a settlement outside its bounded state.
EXPECTED_INITIAL = {
    **EXPECTED_FINAL,
    "LATE_SETTLE": "MISSING_SETTLEMENT",
}

MERCHANTS = [f"M{str(i).zfill(4)}" for i in range(1, 201)]
DEFAULT_BAD_MERCHANT = "M0001"

BANKS = ["HDFC", "ICICI", "SBI", "AXIS", "KOTAK", "YES", "IDFC"]

# A realistic long-tail: small collections dominate, with a few large tickets.
AMOUNTS = [9900, 19900, 49900, 99900, 149900, 250000]


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def epoch_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def cycle(dt: datetime) -> str:
    """NPCI-style settlement cycle label; four cycles a day."""
    return f"{dt:%Y-%m-%d}-C{(dt.hour // 6) + 1}"


def scale_seconds(seconds: float, time_scale: float) -> float:
    """Compress wall-clock delays so a 4-hour SLA is demoable in minutes."""
    return seconds / max(float(time_scale), 1.0)


def decide(sw_amt: Optional[int], st_amt: Optional[int], cb_amt: Optional[int],
           ack: str = "ACK") -> str:
    """
    Mirror of the Flink CASE expression, in precedence order.

    Settlement is checked first because it is the money leg: a missing
    settlement is a finance incident even if the merchant callback also failed.
    """
    if st_amt is None:
        return "MISSING_SETTLEMENT"
    if cb_amt is None or ack != "ACK":
        return "MISSING_CALLBACK"
    if sw_amt != st_amt or sw_amt != cb_amt:
        return "AMOUNT_MISMATCH"
    return "MATCHED"


@dataclass(frozen=True)
class Outbound:
    """A record to put on a topic at (or after) a wall-clock due time."""
    topic: str
    key: str
    value: dict
    due_at: datetime


@dataclass(frozen=True)
class Transaction:
    rrn: str
    merchant_id: str
    fault: str
    expected_status: str
    expected_initial: str
    switch: dict
    callback: Optional[dict]
    settlement: Optional[dict]
    outbound: tuple[Outbound, ...]
    truth: Optional[dict]


class FaultScheduler:
    """
    Mix random faults with a deterministic round-robin.

    Random-only injection is terrible for demos and tests: you can run for ten
    minutes and never see OUT_OF_ORDER.  Every ``force_every`` transactions we
    force the next fault class, which guarantees each recall row has samples.
    """

    def __init__(self, fault_rate: float, force_every: int = 500,
                 rng: Optional[random.Random] = None):
        self.fault_rate = max(0.0, min(float(fault_rate), 1.0))
        self.force_every = max(int(force_every), 0)
        self.rng = rng or random.Random()
        self.counter = 0
        self.forced_idx = 0

    def next(self, rate: Optional[float] = None) -> str:
        self.counter += 1
        if self.force_every and self.counter % self.force_every == 0:
            kind = FAULTS[self.forced_idx % len(FAULTS)]
            self.forced_idx += 1
            return kind
        p = self.fault_rate if rate is None else max(0.0, min(float(rate), 1.0))
        if self.rng.random() < p:
            return self.rng.choice(FAULTS)
        return "NONE"


def choose_merchant(rng: random.Random, bad_merchant: str = DEFAULT_BAD_MERCHANT,
                    bad_rate: float = 0.20) -> str:
    """
    Send a disproportionate share of traffic to one merchant.

    A uniform 3% fault rate spread over 200 merchants never breaches a 10%
    merchant-level alert threshold.  Real incidents are concentrated, so the
    simulator concentrates them too.
    """
    if rng.random() < bad_rate:
        return bad_merchant
    pool = [m for m in MERCHANTS if m != bad_merchant]
    return rng.choice(pool)


def _amount(rng: random.Random) -> int:
    if rng.random() < 0.70:
        return rng.choice(AMOUNTS)
    return rng.randint(100, 1_000_000)


# ---------------------------------------------------------------------------------------
# RRN allocation
# ---------------------------------------------------------------------------------------
# The RRN is the natural key of the whole pipeline: the switch record, the callback, the
# settlement and the reconciliation decision are all joined on it.  Drawing it at random
# from 10^11..10^12 looks fine until the birthday paradox catches up: at 270k
# transactions the collision probability is ~4%, and a collision silently merges two
# different payments into one decision (we saw exactly one such case - a MATCHED
# decision turning into AMOUNT_MISMATCH because a second, unrelated settlement arrived
# under the same RRN).  Allocate RRNs from a random prefix plus a monotonic counter
# instead: unique per process, still 12 digits, no memory cost.
_RRN_PREFIX = random.SystemRandom().randint(1000, 9999)
_rrn_counter = itertools.count(1)


def new_rrn() -> str:
    """Return a 12-digit RRN that is unique for the lifetime of this process."""
    return f"{_RRN_PREFIX}{next(_rrn_counter) % 100_000_000:08d}"


def build_transaction(now: datetime, rng: random.Random, time_scale: float = 60,
                      fault: str = "NONE", merchant_id: Optional[str] = None,
                      bad_merchant: str = DEFAULT_BAD_MERCHANT,
                      bad_merchant_rate: float = 0.20) -> Transaction:
    """Build one transaction plus the wall-clock schedule for its legs."""
    fault = fault or "NONE"
    if fault not in EXPECTED_FINAL:
        raise ValueError(f"unknown fault class: {fault}")

    merchant = merchant_id or choose_merchant(rng, bad_merchant, bad_merchant_rate)
    amount = _amount(rng)
    rrn = new_rrn()

    switch_ts = now
    cb_delay_s = scale_seconds(rng.uniform(0.5, 8.0), time_scale)
    settle_delay_s = scale_seconds(rng.uniform(120, 2700), time_scale)

    if fault == "LATE_SETTLE":
        # Deliberately outside the fast job's 4-hour (scaled) bound.
        settle_delay_s = scale_seconds(rng.uniform(5 * 3600, 20 * 3600), time_scale)

    if fault == "OUT_OF_ORDER":
        # Clock skew: the webhook timestamp is before the switch record.  The
        # SQL interval join allows two minutes (scaled) on the early side.
        cb_ts = switch_ts - timedelta(seconds=1)
        cb_due = switch_ts
    else:
        cb_ts = switch_ts + timedelta(seconds=cb_delay_s)
        cb_due = cb_ts

    settle_ts = switch_ts + timedelta(seconds=settle_delay_s)

    switch = {
        "rrn": rrn,
        "txn_id": str(uuid.uuid4()),
        "merchant_id": merchant,
        "payer_vpa": f"user{rng.randint(1, 999999)}@okaxis",
        "payee_vpa": f"{merchant.lower()}@ybl",
        "amount_paise": amount,
        "status": "SUCCESS",
        "resp_code": "00",
        "event_ts": epoch_ms(switch_ts),
    }

    callback = {
        "rrn": rrn,
        "merchant_id": merchant,
        "order_id": f"ORD-{uuid.uuid4().hex[:10]}",
        "amount_paise": amount,
        "ack_status": "ACK",
        "event_ts": epoch_ms(cb_ts),
    }

    settle_amount = amount
    if fault == "AMOUNT_DRIFT":
        settle_amount = max(1, amount + rng.choice([-200, -100, 1, 50, 300]))

    settlement = {
        "rrn": rrn,
        "utr": f"UTR{rng.randint(10**13, 10**14 - 1)}",
        "merchant_id": merchant,
        "amount_paise": settle_amount,
        "settlement_cycle": cycle(settle_ts),
        "bank_ref": rng.choice(BANKS),
        "event_ts": epoch_ms(settle_ts),
    }

    outbound: list[Outbound] = []

    def add(topic: str, key: str, value: dict, due: datetime) -> None:
        outbound.append(Outbound(topic, key, value, due))

    if fault == "OUT_OF_ORDER":
        add("upi.callbacks", rrn, callback, cb_due)
        add("upi.switch", rrn, switch, switch_ts)
    else:
        add("upi.switch", rrn, switch, switch_ts)
        if fault == "DUP_SWITCH":
            # Same RRN, same event time: a network retry.  Dedup must keep one.
            add("upi.switch", rrn, dict(switch), switch_ts)
        if fault != "MISSING_CALLBACK":
            add("upi.callbacks", rrn, callback, cb_due)

    if fault != "MISSING_SETTLE":
        add("upi.settlements", rrn, settlement, settle_ts)

    truth = None
    if fault != "NONE":
        truth = {
            "rrn": rrn,
            "fault": fault,
            "merchant_id": merchant,
            "expected_status": EXPECTED_FINAL[fault],
            "expected_initial": EXPECTED_INITIAL[fault],
            "injected_at": iso(now),
            "settle_delay_real_s": round(settle_delay_s * time_scale, 3),
            "amount_paise": amount,
            "settlement_amount_paise": settle_amount,
        }

    return Transaction(
        rrn=rrn,
        merchant_id=merchant,
        fault=fault,
        expected_status=EXPECTED_FINAL[fault],
        expected_initial=EXPECTED_INITIAL[fault],
        switch=switch,
        callback=callback if fault != "MISSING_CALLBACK" else None,
        settlement=settlement if fault != "MISSING_SETTLE" else None,
        outbound=tuple(outbound),
        truth=truth,
    )
