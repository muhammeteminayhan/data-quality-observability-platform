"""Severity ordering and the behaviour attached to each level."""

from __future__ import annotations

import pytest

from src.common.severity import Severity, max_severity

pytestmark = pytest.mark.unit


def test_ordering_is_by_rank_not_alphabetical():
    # The bug this guards against: Severity subclasses str, so inheriting
    # str's comparison would make "ERROR" < "WARNING" (alphabetically true,
    # semantically backwards) and every gate decision would be wrong.
    assert Severity.ERROR > Severity.WARNING
    assert Severity.CRITICAL > Severity.ERROR
    assert Severity.INFO < Severity.WARNING
    assert "ERROR" < "WARNING"  # plain strings really do compare this way


def test_full_order():
    levels = [Severity.CRITICAL, Severity.INFO, Severity.ERROR, Severity.WARNING]
    assert sorted(levels) == [
        Severity.INFO,
        Severity.WARNING,
        Severity.ERROR,
        Severity.CRITICAL,
    ]


@pytest.mark.parametrize(
    ("level", "opens", "alerts", "unhealthy", "fails_gate"),
    [
        (Severity.INFO, False, False, False, False),
        (Severity.WARNING, True, True, False, False),
        (Severity.ERROR, True, True, True, False),
        (Severity.CRITICAL, True, True, True, True),
    ],
)
def test_behaviour_flags(level, opens, alerts, unhealthy, fails_gate):
    assert level.opens_incident is opens
    assert level.alerts is alerts
    assert level.marks_unhealthy is unhealthy
    assert level.fails_gate is fails_gate


def test_only_critical_fails_the_gate():
    failing = [level for level in Severity if level.fails_gate]
    assert failing == [Severity.CRITICAL]


def test_parse_accepts_case_and_whitespace():
    assert Severity.parse(" critical ") is Severity.CRITICAL
    assert Severity.parse("Warning") is Severity.WARNING
    assert Severity.parse(Severity.ERROR) is Severity.ERROR


def test_parse_rejects_a_typo_rather_than_downgrading():
    # A silent downgrade would disable a gate: `severity: criticial` becoming
    # INFO means a CRITICAL rule stops blocking anything.
    with pytest.raises(ValueError, match="unknown severity"):
        Severity.parse("criticial")


def test_parse_uses_the_default_only_for_none():
    assert Severity.parse(None, default=Severity.WARNING) is Severity.WARNING
    with pytest.raises(ValueError):
        Severity.parse(None)


def test_max_severity_of_empty_is_the_default():
    # "nothing was found" is INFO - not a pass and not a failure.
    assert max_severity([]) is Severity.INFO
    assert max_severity([], default=Severity.WARNING) is Severity.WARNING


def test_max_severity_picks_the_worst():
    assert max_severity(["INFO", "ERROR", "WARNING"]) is Severity.ERROR
    assert max_severity([Severity.WARNING, Severity.CRITICAL]) is Severity.CRITICAL
