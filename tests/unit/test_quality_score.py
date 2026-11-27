"""The weighted score, and the rules it must never break."""

from __future__ import annotations

import pytest

from src.common.severity import Severity
from src.contracts.models import Contract, ScoreSpec, SourceSpec
from src.observability import quality_score
from src.observability.freshness import FreshnessResult
from src.validation.runner import RuleOutcome

pytestmark = pytest.mark.unit

WEIGHTS = {
    "completeness": 0.30,
    "uniqueness": 0.20,
    "validity": 0.20,
    "consistency": 0.10,
    "freshness": 0.10,
    "volume": 0.10,
}


def contract(**kwargs) -> Contract:
    return Contract(
        dataset="test.dataset",
        version="1.0.0",
        source=SourceSpec(type="postgres", location="raw.test"),
        **kwargs,
    )


def outcome(dimension, success, severity=Severity.ERROR, total=1000, unexpected=0):
    return RuleOutcome(
        rule_name=f"{dimension}_rule",
        dimension=dimension,
        severity=severity,
        expectation_type="expect_something",
        column="c",
        success=success,
        element_count=total,
        unexpected_count=unexpected,
    )


def freshness(status, lag, severity):
    return FreshnessResult(
        dataset="test.dataset",
        timestamp_column="ts",
        timestamp_source="event_time",
        max_timestamp=None,
        reference_time=None,
        lag_seconds=lag,
        warning_after_seconds=3600,
        critical_after_seconds=7200,
        status=status,
        severity=severity,
        detail="",
    )


FOUR_PASSING = [
    outcome("completeness", True),
    outcome("uniqueness", True),
    outcome("validity", True),
    outcome("consistency", True),
]


def test_all_passing_scores_100_and_is_healthy():
    score = quality_score.compute(contract(), rule_outcomes=FOUR_PASSING)
    assert score.overall_score == 100.0
    assert score.health_status == "healthy"
    assert score.has_critical_failure is False


def test_a_critical_failure_is_never_healthy_however_high_the_score():
    # One failing CRITICAL rule out of 100,000 rows: the score stays very
    # high and the dataset must still be unhealthy.
    score = quality_score.compute(
        contract(),
        rule_outcomes=[
            outcome("completeness", False, Severity.CRITICAL, 100_000, 1),
            outcome("uniqueness", True),
            outcome("validity", True),
            outcome("consistency", True),
        ],
    )
    assert score.overall_score > 95.0
    assert score.has_critical_failure is True
    assert score.health_status == "unhealthy"


def test_unmeasured_dimensions_are_excluded_not_scored_as_100():
    score = quality_score.compute(
        contract(),
        rule_outcomes=[outcome("completeness", False, Severity.ERROR, 100, 50)],
    )
    assert score.dimensions_measured == 1
    assert sorted(score.unmeasured_dimensions) == [
        "consistency",
        "freshness",
        "uniqueness",
        "validity",
        "volume",
    ]
    # 50 of 100 rows unexpected -> the one measured dimension scores 50, and
    # being the only one measured it is also the overall score.
    assert score.overall_score == pytest.approx(50.0, abs=0.01)


def test_weights_are_renormalised_over_the_measured_dimensions():
    score = quality_score.compute(
        contract(score=ScoreSpec(weights=dict(WEIGHTS))),
        rule_outcomes=FOUR_PASSING,
        freshness_result=freshness("ok", 0, Severity.INFO),
        volume_result=None,
    )
    assert score.weights_used["completeness"] == pytest.approx(0.30 / 0.90, abs=1e-6)
    assert score.weights_used["freshness"] == pytest.approx(0.10 / 0.90, abs=1e-6)
    # Each weight is stored rounded to 6 decimals, so the sum is 0.999999
    # rather than exactly 1. The rounding is deliberate - the stored value
    # is for humans reading weights_used on a dashboard.
    assert sum(score.weights_used.values()) == pytest.approx(1.0, abs=1e-5)
    assert score.unmeasured_dimensions == ["volume"]


def test_freshness_zero_costs_exactly_its_renormalised_weight():
    score = quality_score.compute(
        contract(score=ScoreSpec(weights=dict(WEIGHTS))),
        rule_outcomes=FOUR_PASSING,
        freshness_result=freshness("critical", 99_999, Severity.CRITICAL),
    )
    # Five dimensions measured (volume absent); freshness scores 0.
    assert score.overall_score == pytest.approx(0.80 / 0.90 * 100, abs=0.01)
    assert score.has_critical_failure is True
    assert score.health_status == "unhealthy"


def test_nothing_measured_is_unknown_not_perfect():
    score = quality_score.compute(contract(), rule_outcomes=[])
    assert score.dimensions_measured == 0
    assert score.health_status == "unknown"
    assert score.overall_score == 0.0


def test_a_rule_that_could_not_run_counts_as_a_full_failure():
    broken = RuleOutcome(
        rule_name="sql_rule",
        dimension="consistency",
        severity=Severity.ERROR,
        expectation_type="unexpected_rows",
        column=None,
        success=False,
        element_count=None,
        unexpected_count=None,
        exception_message="relation does not exist",
    )
    score = quality_score.compute(contract(), rule_outcomes=[broken])
    assert score.dimension_scores["consistency"] == 0.0


def test_degraded_sits_between_the_two_thresholds():
    spec = ScoreSpec(weights=dict(WEIGHTS), healthy_at=90.0, degraded_at=70.0)
    score = quality_score.compute(
        contract(score=spec),
        rule_outcomes=[
            outcome("completeness", False, Severity.WARNING, 100, 50),
            outcome("uniqueness", True),
            outcome("validity", True),
            outcome("consistency", True),
        ],
    )
    # completeness scores 50; the other three score 100. Freshness and
    # volume are unmeasured, so the weights renormalise over 0.80:
    #   (50*0.30 + 100*0.20 + 100*0.20 + 100*0.10) / 0.80 = 81.25
    assert score.overall_score == pytest.approx(81.25, abs=0.01)
    assert 70.0 <= score.overall_score < 90.0
    assert score.health_status == "degraded"
    assert score.has_critical_failure is False
