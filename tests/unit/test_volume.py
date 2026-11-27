"""The robust volume baseline."""

from __future__ import annotations

import statistics

import pytest

from src.observability.volume import MAD_TO_SIGMA, MIN_SPREAD_FRACTION

pytestmark = pytest.mark.unit


def robust_z(count: int, history: list[int]) -> float:
    """The same statistic volume.check computes, isolated for testing."""
    median = float(statistics.median(history))
    mad = float(statistics.median([abs(v - median) for v in history]))
    spread = max(mad, median * MIN_SPREAD_FRACTION, 1.0)
    return MAD_TO_SIGMA * (count - median) / spread


WEEKDAYS = [50_000, 51_000, 49_500, 50_500, 50_200, 49_800, 50_100]
WEEKENDS = [31_000, 30_000, 30_500, 29_800, 30_200]


def test_a_count_at_the_median_scores_zero():
    assert robust_z(50_100, WEEKDAYS) == pytest.approx(0.0, abs=0.2)


def test_normal_weekday_variation_stays_well_inside_the_warning_band():
    for count in WEEKDAYS:
        assert abs(robust_z(count, WEEKDAYS)) < 3.5


def test_a_weekend_compared_against_weekdays_looks_like_a_disaster():
    # This is the false alarm `same_day_type` exists to prevent: a perfectly
    # normal Sunday measured against a weekday baseline.
    assert abs(robust_z(30_000, WEEKDAYS)) > 6.0


def test_the_same_weekend_compared_against_weekends_is_unremarkable():
    assert abs(robust_z(30_000, WEEKENDS)) < 3.5


def test_a_real_collapse_is_critical_on_either_baseline():
    assert abs(robust_z(5_000, WEEKDAYS)) > 6.0
    assert abs(robust_z(5_000, WEEKENDS)) > 6.0


def test_a_doubling_is_critical():
    assert abs(robust_z(100_000, WEEKDAYS)) > 6.0


def test_the_median_is_not_dragged_by_one_outlier():
    # A mean would be pulled so far by a 10x spike that the next genuine
    # spike would look normal. The median is why the baseline is robust.
    polluted = WEEKDAYS + [500_000]
    assert statistics.median(polluted) == pytest.approx(
        statistics.median(WEEKDAYS), rel=0.05
    )
    assert abs(robust_z(50_100, polluted)) < 3.5


def test_a_perfectly_stable_history_still_yields_a_finite_score():
    # MAD is 0 when every observation is identical, so the spread floor is
    # what keeps the z-score defined at all.
    stable = [49_000] * 10
    assert robust_z(49_000, stable) == pytest.approx(0.0, abs=1e-9)
    small_move = robust_z(49_100, stable)
    assert small_move != float("inf")
    assert abs(small_move) < 3.5


def test_a_large_move_against_a_stable_history_is_still_caught():
    stable = [49_000] * 10
    assert abs(robust_z(98_000, stable)) > 6.0
