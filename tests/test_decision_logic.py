"""The decision table is the heart of the pipeline; pin it with unit tests."""
import pytest

from generator.model import decide


@pytest.mark.parametrize(
    "sw,st,cb,ack,expected",
    [
        (9900, 9900, 9900, "ACK", "MATCHED"),
        (9900, 9900, None, "ACK", "MISSING_CALLBACK"),
        (9900, 9900, 9900, "NACK", "MISSING_CALLBACK"),
        (9900, 9900, 9900, "TIMEOUT", "MISSING_CALLBACK"),
        (9900, None, 9900, "ACK", "MISSING_SETTLEMENT"),
        (9900, 9850, 9900, "ACK", "AMOUNT_MISMATCH"),
        (9900, 9900, 9800, "ACK", "AMOUNT_MISMATCH"),
        (9900, 9850, 9800, "ACK", "AMOUNT_MISMATCH"),
        # Precedence: settlement is the money leg, so a missing settlement wins
        # over a simultaneous callback failure or amount drift.
        (9900, None, None, "NACK", "MISSING_SETTLEMENT"),
        (9900, None, 9800, "ACK", "MISSING_SETTLEMENT"),
    ],
)
def test_decision(sw, st, cb, ack, expected):
    assert decide(sw, st, cb, ack) == expected


def test_one_paisa_is_still_a_break():
    assert decide(100, 101, 100) == "AMOUNT_MISMATCH"
