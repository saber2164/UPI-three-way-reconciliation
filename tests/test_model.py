"""
Unit tests for the simulator's fault semantics.  These run without Kafka,
Postgres, Docker, or Flink — the model module is deliberately dependency-free.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import random

import pytest

from generator.model import (
    EXPECTED_FINAL,
    EXPECTED_INITIAL,
    FAULTS,
    FaultScheduler,
    build_transaction,
    choose_merchant,
    decide,
)

NOW = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)


def txn(fault: str, seed: int = 7, time_scale: float = 60):
    return build_transaction(
        NOW,
        random.Random(seed),
        time_scale=time_scale,
        fault=fault,
        merchant_id="M0042",
    )


def by_topic(transaction, topic):
    return [o for o in transaction.outbound if o.topic == topic]


def test_expected_maps_cover_every_fault_and_happy_path():
    assert set(EXPECTED_FINAL) == {"NONE", *FAULTS}
    assert set(EXPECTED_INITIAL) == set(EXPECTED_FINAL)
    for fault in FAULTS:
        assert fault in EXPECTED_FINAL
    # Only LATE_SETTLE changes between first pass and correction.
    for fault in EXPECTED_FINAL:
        if fault != "LATE_SETTLE":
            assert EXPECTED_INITIAL[fault] == EXPECTED_FINAL[fault]
    assert EXPECTED_INITIAL["LATE_SETTLE"] == "MISSING_SETTLEMENT"
    assert EXPECTED_FINAL["LATE_SETTLE"] == "MATCHED"


def test_unknown_fault_rejected():
    with pytest.raises(ValueError):
        txn("NO_SUCH_FAULT")


def test_happy_path_has_three_legs_and_no_truth():
    t = txn("NONE")
    assert t.expected_status == "MATCHED"
    assert t.truth is None
    assert len(by_topic(t, "upi.switch")) == 1
    assert len(by_topic(t, "upi.callbacks")) == 1
    assert len(by_topic(t, "upi.settlements")) == 1
    assert t.switch["amount_paise"] == t.callback["amount_paise"]
    assert t.switch["amount_paise"] == t.settlement["amount_paise"]
    assert isinstance(t.switch["amount_paise"], int)
    assert t.switch["amount_paise"] > 0


@pytest.mark.parametrize("fault", FAULTS)
def test_every_injected_fault_has_truth_and_expected_status(fault):
    t = txn(fault)
    assert t.fault == fault
    assert t.truth is not None
    assert t.truth["expected_status"] == EXPECTED_FINAL[fault]
    assert t.truth["expected_initial"] == EXPECTED_INITIAL[fault]
    assert t.truth["rrn"] == t.rrn
    assert t.truth["amount_paise"] == t.switch["amount_paise"]


def test_duplicate_switch_is_same_rrn_and_not_money():
    t = txn("DUP_SWITCH")
    switches = by_topic(t, "upi.switch")
    assert len(switches) == 2
    assert {s.value["rrn"] for s in switches} == {t.rrn}
    assert len(by_topic(t, "upi.settlements")) == 1
    assert t.expected_status == "MATCHED"


def test_amount_drift_changes_settlement_only():
    t = txn("AMOUNT_DRIFT")
    assert t.settlement["amount_paise"] != t.switch["amount_paise"]
    assert t.callback["amount_paise"] == t.switch["amount_paise"]
    assert decide(
        t.switch["amount_paise"],
        t.settlement["amount_paise"],
        t.callback["amount_paise"],
    ) == "AMOUNT_MISMATCH"


def test_missing_settlement_emits_inventory_only():
    t = txn("MISSING_SETTLE")
    assert t.settlement is None
    assert by_topic(t, "upi.settlements") == []
    assert len(by_topic(t, "upi.callbacks")) == 1
    assert t.expected_initial == "MISSING_SETTLEMENT"
    assert t.expected_status == "MISSING_SETTLEMENT"


def test_missing_callback_emits_settlement_only():
    t = txn("MISSING_CALLBACK")
    assert t.callback is None
    assert by_topic(t, "upi.callbacks") == []
    assert len(by_topic(t, "upi.settlements")) == 1
    assert decide(
        t.switch["amount_paise"],
        t.settlement["amount_paise"],
        None,
    ) == "MISSING_CALLBACK"


def test_late_settlement_is_outside_fast_bound() -> None:
    t = txn("LATE_SETTLE")
    sw_ts = t.switch["event_ts"]
    st_ts = t.settlement["event_ts"]
    # 4 hours scaled by 60 -> 240,000 ms.
    assert st_ts - sw_ts > 4 * 60 * 60 * 1000 / 60
    assert t.expected_initial == "MISSING_SETTLEMENT"
    assert t.expected_status == "MATCHED"
    assert 5 * 3600 <= t.truth["settle_delay_real_s"] <= 20 * 3600


def test_out_of_order_callback_precedes_switch():
    t = txn("OUT_OF_ORDER")
    cb = by_topic(t, "upi.callbacks")[0]
    sw = by_topic(t, "upi.switch")[0]
    assert cb.value["event_ts"] < sw.value["event_ts"]
    # The callback is still due for delivery when the switch arrives.
    assert cb.due_at == sw.due_at
    assert t.expected_status == "MATCHED"


def test_on_time_settlement_is_inside_fast_bound_at_default_scale():
    t = txn("NONE")
    delay_ms = t.settlement["event_ts"] - t.switch["event_ts"]
    # 120..2700 simulated seconds at 60x -> 2..45 real seconds.
    assert 2_000 <= delay_ms <= 45_000


def test_time_scale_changes_wall_clock_but_not_truth_delay():
    scaled = txn("LATE_SETTLE", time_scale=60)
    production = txn("LATE_SETTLE", time_scale=1)
    assert scaled.truth["settle_delay_real_s"] == pytest.approx(
        (production.settlement["event_ts"] - production.switch["event_ts"]) / 1000,
        # the truth row stores the delay rounded to milliseconds
        abs=0.001,
    )
    assert production.settlement["event_ts"] - production.switch["event_ts"] > scaled.settlement["event_ts"] - scaled.switch["event_ts"]


def test_forced_scheduler_covers_every_fault_class():
    scheduler = FaultScheduler(0.0, force_every=1, rng=random.Random(1))
    assert [scheduler.next() for _ in range(len(FAULTS))] == list(FAULTS)


def test_scheduler_respects_rate_extremes():
    never = FaultScheduler(0.0, force_every=0, rng=random.Random(1))
    always = FaultScheduler(1.0, force_every=0, rng=random.Random(1))
    assert all(never.next() == "NONE" for _ in range(100))
    assert all(always.next() in FAULTS for _ in range(100))


def test_choose_merchant_concentrates_bad_merchant():
    only_bad = random.Random(1)
    only_good = random.Random(1)
    assert all(
        choose_merchant(only_bad, bad_merchant="M0001", bad_rate=1.0) == "M0001"
        for _ in range(20)
    )
    assert all(
        choose_merchant(only_good, bad_merchant="M0001", bad_rate=0.0) != "M0001"
        for _ in range(20)
    )


def test_custom_bad_merchant_is_excluded_from_normal_traffic():
    rng = random.Random(3)
    picks = {choose_merchant(rng, bad_merchant="M0002", bad_rate=0.0) for _ in range(100)}
    assert "M0002" not in picks


def test_rrns_are_unique_and_twelve_digits():
    """The RRN is the natural key of the whole pipeline, so it must not collide.

    Drawing it at random from 10^11..10^12 collides roughly every few hundred
    thousand transactions (birthday paradox), and a collision silently merges two
    payments into one decision.
    """
    from generator.model import new_rrn

    rrns = [new_rrn() for _ in range(50_000)]
    assert len(set(rrns)) == len(rrns)
    assert all(len(r) == 12 and r.isdigit() for r in rrns)
