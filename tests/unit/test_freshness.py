"""Freshness classification, including the refusal to guess."""

from __future__ import annotations

import pytest

from src.common.severity import Severity
from src.observability import freshness

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    ("lag", "expected_status", "expected_severity"),
    [
        (0, "ok", Severity.INFO),
        (3599, "ok", Severity.INFO),
        (3600, "warning", Severity.WARNING),
        (7199, "warning", Severity.WARNING),
        (7200, "critical", Severity.CRITICAL),
        (999_999, "critical", Severity.CRITICAL),
    ],
)
def test_classification_at_and_around_the_thresholds(lag, expected_status, expected_severity):
    status, severity = freshness._classify(lag, warn=3600, crit=7200)
    assert status == expected_status
    assert severity is expected_severity


def test_no_lag_at_all_is_unknown_never_ok():
    # A dataset that could not be measured must never read as fresh.
    status, _ = freshness._classify(None, warn=3600, crit=7200)
    assert status == "unknown"


def test_absent_thresholds_never_raise_an_alarm():
    status, severity = freshness._classify(100_000, warn=None, crit=None)
    assert status == "ok"
    assert severity is Severity.INFO
