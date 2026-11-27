"""PSI and chi-square, and the refusal to conclude from a small sample."""

from __future__ import annotations

import pandas as pd
import pytest

from src.common.severity import Severity
from src.contracts.models import CategoricalDriftSpec, NumericDriftSpec
from src.observability import distribution

pytestmark = pytest.mark.unit


def test_psi_of_an_identical_distribution_is_zero():
    counts = [100, 200, 300, 400]
    assert distribution.population_stability_index(counts, counts) == pytest.approx(0.0, abs=1e-9)


def test_psi_grows_with_the_size_of_the_shift():
    reference = [400, 300, 200, 100]
    small = distribution.population_stability_index(reference, [380, 310, 210, 100])
    large = distribution.population_stability_index(reference, [100, 200, 300, 400])
    assert 0 < small < large


def test_psi_is_symmetric():
    a, b = [500, 300, 200], [200, 300, 500]
    assert distribution.population_stability_index(a, b) == pytest.approx(
        distribution.population_stability_index(b, a), abs=1e-9
    )


def test_psi_stays_finite_when_a_bin_empties():
    # Without the epsilon floor, ln(0) would make this infinite and any
    # threshold comparison meaningless.
    value = distribution.population_stability_index([100, 100], [200, 0])
    assert value > 0
    assert value != float("inf")


def test_psi_rejects_mismatched_bins():
    with pytest.raises(ValueError, match="same number of bins"):
        distribution.population_stability_index([1, 2, 3], [1, 2])


def profile(values, bins=4, edges=None):
    return distribution.profile_numeric(
        "ds", "col", pd.Series(values, dtype="float64"), bins, edges=edges
    )


def test_a_small_sample_is_inconclusive_and_claims_nothing():
    spec = NumericDriftSpec(column="col", bins=4, warning=0.1, critical=0.25)
    reference = profile(list(range(1000)))
    # 20 rows, wildly different values - a naive PSI would be large.
    current = profile([900.0] * 20, edges=reference.bin_edges)

    result = distribution.compare_numeric(reference, current, spec, min_sample=500)
    assert result.status == "inconclusive"
    assert result.is_significant is False
    assert result.statistic is None
    assert "sample too small" in result.detail


def test_a_real_shift_on_a_large_sample_is_significant():
    spec = NumericDriftSpec(column="col", bins=4, warning=0.1, critical=0.25)
    reference = profile(list(range(2000)))
    shifted = profile([float(v) for v in range(1800, 3800)], edges=reference.bin_edges)

    result = distribution.compare_numeric(reference, shifted, spec, min_sample=500)
    assert result.status in ("warning", "critical")
    assert result.is_significant is True
    assert result.statistic > spec.warning


def test_no_reference_profile_means_no_claim():
    spec = NumericDriftSpec(column="col")
    current = profile(list(range(2000)))
    result = distribution.compare_numeric(None, current, spec, min_sample=10)
    assert result.status == "inconclusive"
    assert result.is_significant is False
    assert "no reference profile" in result.detail


def test_binning_uses_quantiles_so_a_skewed_column_is_not_one_bin():
    # Log-normal-ish CONTINUOUS data. With equal-width bins almost every row
    # lands in the first bucket and PSI goes blind to anything but extremes;
    # quantile edges spread the rows across the bins instead.
    import math

    values = [math.exp(i / 120.0) for i in range(1000)]
    built = profile(values, bins=4)
    assert built.bin_counts is not None
    non_empty = [count for count in built.bin_counts if count > 0]
    assert len(non_empty) == 4
    # Quantile binning puts roughly a quarter of the rows in each bin, which
    # equal-width binning on this data would not.
    assert max(built.bin_counts) < 2 * min(built.bin_counts)


def test_duplicate_quantile_edges_collapse_rather_than_producing_empty_bins():
    # A column with only a handful of distinct values cannot fill four
    # quantile bins: the edges coincide and are de-duplicated. Fewer, honest
    # bins is the right outcome - empty bins would make PSI jump on noise.
    values = [1.0] * 500 + [10.0] * 300 + [100.0] * 150 + [10_000.0] * 50
    built = profile(values, bins=4)
    assert built.bin_edges == sorted(set(built.bin_edges))
    assert sum(built.bin_counts) == len(values)


def test_values_outside_the_reference_range_still_land_in_a_bin():
    reference = profile([float(v) for v in range(100)], bins=4)
    # Everything far above the reference maximum.
    current = profile([5000.0] * 100, edges=reference.bin_edges)
    assert sum(current.bin_counts) == 100


def categorical(counts: dict[str, int]):
    data = []
    for value, count in counts.items():
        data.extend([value] * count)
    return distribution.profile_categorical("ds", "col", pd.Series(data, dtype="object"), 20)


def test_chi_square_finds_no_shift_in_an_identical_mix():
    spec = CategoricalDriftSpec(column="col", alpha=0.01, min_expected_frequency=5)
    mix = {"a": 500, "b": 300, "c": 200}
    result = distribution.compare_categorical(
        categorical(mix), categorical(mix), spec, min_sample=100
    )
    assert result.status == "ok"
    assert result.is_significant is False
    assert result.p_value > spec.alpha


def test_chi_square_finds_a_collapsed_mix():
    spec = CategoricalDriftSpec(column="col", alpha=0.01, min_expected_frequency=5)
    reference = categorical({"a": 400, "b": 300, "c": 300})
    collapsed = categorical({"a": 950, "b": 30, "c": 20})
    result = distribution.compare_categorical(reference, collapsed, spec, min_sample=100)
    assert result.is_significant is True
    assert result.p_value < spec.alpha
    assert result.degrees_of_freedom == 2


def test_chi_square_is_not_fooled_by_a_change_in_total_rows_alone():
    # Same proportions, ten times the rows. The test is about the MIX, so
    # this must not register as drift.
    spec = CategoricalDriftSpec(column="col", alpha=0.01, min_expected_frequency=5)
    reference = categorical({"a": 100, "b": 200, "c": 300})
    bigger = categorical({"a": 1000, "b": 2000, "c": 3000})
    result = distribution.compare_categorical(reference, bigger, spec, min_sample=100)
    assert result.is_significant is False


def test_a_small_noisy_reference_does_not_manufacture_drift():
    """Regression: a 1,000-row reference against a 10,000-row current.

    These are the real `raw.customers.state` profiles the platform stored on
    two clean loads of the SAME generator (profile 648 vs 664). The mixes are
    both uniform over 20 states; the only difference is sampling noise.

    The first implementation compared them as a goodness-of-fit test against
    the reference proportions and reported chi2=99.415 on 19 df, p=6.8e-13 -
    drift, at WARNING, on clean data. The reference's own +/-7-count noise was
    being scaled up tenfold and then charged to the current sample.

    A 2 x k homogeneity test on the same counts gives chi2=9.012, p=0.973.
    """
    spec = CategoricalDriftSpec(column="col", alpha=0.01, min_expected_frequency=5)
    reference = categorical(
        {
            "AZ": 50, "CA": 59, "FL": 60, "GA": 49, "IL": 42,
            "IN": 43, "MA": 54, "MD": 44, "MI": 53, "MO": 50,
            "NC": 46, "NJ": 44, "NY": 43, "OH": 43, "PA": 63,
            "TN": 50, "TX": 54, "VA": 59, "WA": 52, "WI": 42,
        }
    )
    current = categorical(
        {
            "AZ": 507, "CA": 529, "FL": 506, "GA": 487, "IL": 495,
            "IN": 494, "MA": 543, "MD": 489, "MI": 488, "MO": 527,
            "NC": 475, "NJ": 474, "NY": 496, "OH": 467, "PA": 551,
            "TN": 459, "TX": 525, "VA": 546, "WA": 469, "WI": 473,
        }
    )
    assert reference.sample_size == 1000
    assert current.sample_size == 10000

    result = distribution.compare_categorical(reference, current, spec, min_sample=500)

    assert result.status == "ok"
    assert result.is_significant is False
    assert result.degrees_of_freedom == 19
    assert result.statistic == pytest.approx(9.012, abs=0.01)
    assert result.p_value == pytest.approx(0.973, abs=0.001)


def test_a_real_shift_is_still_caught_with_an_uneven_sample_split():
    """The homogeneity test must not be so forgiving that it misses drift.

    Same lopsided 1,000 vs 10,000 split as the regression above, but the
    current mix really has moved: one category takes a third of the rows.
    """
    spec = CategoricalDriftSpec(column="col", alpha=0.01, min_expected_frequency=5)
    reference = categorical({"AZ": 250, "CA": 250, "FL": 250, "GA": 250})
    shifted = categorical({"AZ": 3400, "CA": 2200, "FL": 2200, "GA": 2200})

    result = distribution.compare_categorical(reference, shifted, spec, min_sample=500)

    assert result.status == "warning"
    assert result.is_significant is True
    assert result.severity is Severity.WARNING
    assert result.p_value < spec.alpha


def test_an_empty_current_mix_is_not_reported_as_drift():
    """All-null on one side is a measurement failure, not a mix change."""
    spec = CategoricalDriftSpec(column="col", alpha=0.01, min_expected_frequency=5)
    reference = categorical({"a": 600, "b": 400})
    empty = distribution.profile_categorical(
        "ds", "col", pd.Series([None] * 1000, dtype="object"), 20
    )
    result = distribution.compare_categorical(reference, empty, spec, min_sample=100)
    assert result.is_significant is False
    assert result.status == "inconclusive"


def test_sparse_categories_are_pooled_and_reported():
    spec = CategoricalDriftSpec(column="col", alpha=0.01, min_expected_frequency=50)
    reference = categorical({"a": 500, "b": 490, "rare": 10})
    current = categorical({"a": 500, "b": 490, "rare": 10})
    result = distribution.compare_categorical(reference, current, spec, min_sample=100)
    assert "pooled" in result.detail
    assert distribution.OTHER_BUCKET in result.detail


def test_categorical_small_sample_is_also_inconclusive():
    spec = CategoricalDriftSpec(column="col")
    reference = categorical({"a": 900, "b": 100})
    tiny = categorical({"a": 5, "b": 25})
    result = distribution.compare_categorical(reference, tiny, spec, min_sample=500)
    assert result.status == "inconclusive"
    assert result.is_significant is False
