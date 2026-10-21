"""
Integration checks against a running standalone stack.

They are marked ``integration`` and skip when Postgres is not reachable, so
``pytest tests -m "not integration"`` remains a laptop-only unit suite.
"""
from __future__ import annotations

import math
import os

import pytest

pytest.importorskip("psycopg2")
import psycopg2  # noqa: E402

pytestmark = pytest.mark.integration

DSN = os.environ.get(
    "POSTGRES_DSN",
    "postgresql://app:app@localhost:5432/fintech",
)
TIME_SCALE = float(os.environ.get("TIME_SCALE", "60"))

# The fast job only declares a settlement missing once the settlement watermark
# has passed switch_ts + 4 h (production units).  A decision therefore cannot
# exist before that bound, no matter how old the transaction is.
SETTLE_BOUND_REAL_S = 4 * 3600


@pytest.fixture(scope="module")
def cur():
    try:
        conn = psycopg2.connect(DSN, connect_timeout=3)
    except Exception as exc:  # noqa: BLE001 - environment dependent
        pytest.skip(f"Postgres not reachable at {DSN}: {exc}")
    conn.autocommit = True
    with conn.cursor() as cursor:
        yield cursor
    conn.close()


def scalar(cur, sql: str, params=None):
    cur.execute(sql, params or ())
    row = cur.fetchone()
    return row[0] if row else None


def aged_minutes(real_seconds: float) -> int:
    """
    Give the scaled demo enough wall-clock time to see the decision.

    ``real_seconds`` is the production-time delay of the leg under test.  The
    demo compresses it by TIME_SCALE, so a 45 minute settlement delay is 45
    seconds of wall clock at TIME_SCALE=60.  The decision can only appear after
    the settlement bound has elapsed as well, hence the max().
    """
    need = max(float(real_seconds), SETTLE_BOUND_REAL_S)
    return max(5, int(math.ceil(need / max(TIME_SCALE, 1.0) / 60.0)) + 5)


def recall(cur, fault: str, expected_status: str, age_sql: str, age_params=()):
    n = scalar(
        cur,
        f"""
        select count(*)
        from recon.ground_truth g
        where g.fault = %s and g.injected_at < now() - ({age_sql})
        """,
        (fault, *age_params),
    )
    if not n:
        pytest.skip(f"no aged {fault} samples yet")
    hit = scalar(
        cur,
        f"""
        select count(*)
        from recon.ground_truth g
        join recon.match_results m using (rrn)
        where g.fault = %s
          and g.injected_at < now() - ({age_sql})
          and m.match_status = %s
        """,
        (fault, *age_params, expected_status),
    )
    return (hit or 0) / n


@pytest.mark.parametrize(
    "fault,status,real_seconds",
    [
        ("AMOUNT_DRIFT", "AMOUNT_MISMATCH", 45 * 60),
        ("MISSING_CALLBACK", "MISSING_CALLBACK", 45 * 60),
        ("MISSING_SETTLE", "MISSING_SETTLEMENT", 45 * 60),
        ("DUP_SWITCH", "MATCHED", 45 * 60),
        ("OUT_OF_ORDER", "MATCHED", 45 * 60),
    ],
)
def test_final_recall(cur, fault, status, real_seconds):
    minutes = aged_minutes(real_seconds)
    r = recall(
        cur,
        fault,
        status,
        "make_interval(mins => %s)",
        (minutes,),
    )
    assert r >= 0.95, f"{fault} final recall {r:.2%} < 95%"


def test_late_settlement_initially_missing_then_corrected(cur):
    minutes = aged_minutes(20 * 3600)
    n = scalar(
        cur,
        """
        select count(*)
        from recon.ground_truth g
        where g.fault = 'LATE_SETTLE'
          and g.injected_at < now() - make_interval(mins => %s)
        """,
        (minutes,),
    )
    if not n:
        pytest.skip("no aged LATE_SETTLE samples yet")

    initial = scalar(
        cur,
        """
        select count(*)
        from recon.decision_audit a
        join recon.ground_truth g using (rrn)
        where g.fault = 'LATE_SETTLE'
          and g.injected_at < now() - make_interval(mins => %s)
          and a.match_status = 'MISSING_SETTLEMENT'
        """,
        (minutes,),
    )
    assert initial and initial > 0, "late settlement never surfaced as MISSING_SETTLEMENT"

    corrected = scalar(
        cur,
        """
        select count(*)
        from recon.ground_truth g
        join recon.match_results m using (rrn)
        where g.fault = 'LATE_SETTLE'
          and g.injected_at < now() - make_interval(mins => %s)
          and m.match_status = 'MATCHED'
          and m.settle_amount is not null
        """,
        (minutes,),
    )
    assert corrected and corrected / n >= 0.95, (
        f"late correction recall {(corrected or 0) / n:.2%} < 95%"
    )


def test_duplicate_switch_does_not_create_extra_exception(cur):
    minutes = aged_minutes(45 * 60)
    bad = scalar(
        cur,
        """
        select count(*)
        from recon.ground_truth g
        join recon.match_results m using (rrn)
        where g.fault = 'DUP_SWITCH'
          and g.injected_at < now() - make_interval(mins => %s)
          and m.match_status <> 'MATCHED'
        """,
        (minutes,),
    )
    assert bad == 0


def test_control_total_identity_holds(cur):
    bad = scalar(
        cur,
        """
        select count(*)
        from (
          select rrn,
                 switch_amount,
                 settle_amount,
                 callback_amount,
                 match_status
          from recon.match_results
          where match_status <> 'MATCHED'
            and switch_amount is not null
        ) x
        where (match_status = 'MISSING_SETTLEMENT' and settle_amount is not null)
           or (match_status = 'AMOUNT_MISMATCH' and settle_amount = switch_amount)
        """,
    )
    assert bad == 0
