"""Distribution drift: PSI for numeric columns, chi-square for categorical.

The honesty rules this module exists to enforce
-----------------------------------------------
* **A small sample proves nothing.** If either side has fewer than the
  contract `min_sample_size` rows, the test is NOT run. The result is recorded
  with status `inconclusive` and `is_significant = false`. A PSI of 0.9 on 20
  rows is noise, and reporting it as drift is how a drift detector loses the
  trust of the people reading it.

* **Binning is fixed by the reference.** Bin edges are computed once from the
  reference profile and stored with it. Re-binning each side independently
  would compare different buckets and manufacture drift from nothing.

* **Chi-square needs populated cells.** Categories whose smallest EXPECTED
  cell falls below the contract threshold are pooled into a single `__other__`
  bucket, because the chi-square approximation breaks down on sparse cells.
  If pooling leaves fewer than two categories the test is inconclusive.

* **Both sides are samples.** The categorical test is a chi-square test of
  HOMOGENEITY on a 2 x k table, not a goodness-of-fit test against the
  reference proportions. Treating a sampled reference as a known distribution
  ignores its own sampling error and manufactures drift whenever the reference
  is small; `compare_categorical` records the measured difference.

* **A statistic is not a verdict.** `is_significant` is true only when the
  sample sizes were sufficient AND the statistic crossed a contract threshold
  (PSI) or the p-value fell below alpha (chi-square).

PSI
---
    PSI = sum over bins of (actual% - expected%) * ln(actual% / expected%)

Conventional reading: < 0.1 no real shift, 0.1-0.25 moderate, > 0.25 major.
Those are the contract defaults. Empty bins are floored at a small epsilon so
the logarithm stays finite; the floor is reported in the detail because it
bounds how large the statistic can get.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date
from typing import Any

import numpy as np
import pandas as pd

from src.common.db import fetch_all, qualified_identifier, source_connection
from src.common.logging import get_logger
from src.common.severity import Severity
from src.common.storage import read_partition
from src.contracts.models import CategoricalDriftSpec, Contract, NumericDriftSpec

LOGGER = get_logger(__name__)

# Floor applied to an empty bin so ln() stays finite.
PSI_EPSILON = 1e-6
OTHER_BUCKET = "__other__"


@dataclass
class ColumnProfile:
    dataset: str
    column: str
    kind: str  # numeric | categorical
    sample_size: int
    null_count: int
    distinct_count: int | None = None
    mean: float | None = None
    stddev: float | None = None
    minimum: float | None = None
    p25: float | None = None
    median: float | None = None
    p75: float | None = None
    maximum: float | None = None
    bin_edges: list[float] | None = None
    bin_counts: list[int] | None = None
    category_counts: dict[str, int] | None = None


@dataclass
class DriftResult:
    dataset: str
    column: str
    kind: str
    method: str
    reference_sample_size: int | None
    current_sample_size: int
    min_sample_size: int
    statistic: float | None
    p_value: float | None
    degrees_of_freedom: int | None
    threshold_warning: float | None
    threshold_critical: float | None
    status: str  # ok | warning | critical | inconclusive
    severity: Severity
    is_significant: bool
    detail: str
    reference_profile_id: int | None = None
    current_profile_id: int | None = None
    extras: dict[str, Any] = field(default_factory=dict)


def _load_frame(contract: Contract, partition_date: date, columns: list[str]) -> pd.DataFrame:
    """Read just the columns the drift specs need."""
    if contract.source.type == "parquet":
        frame = read_partition(contract.source.location, partition_date.isoformat())
        present = [c for c in columns if c in frame.columns]
        return frame[present] if present else pd.DataFrame()

    from psycopg import sql

    statement = sql.SQL("SELECT {cols} FROM {tbl}").format(
        cols=sql.SQL(", ").join(sql.Identifier(c) for c in columns),
        tbl=qualified_identifier(contract.source.location),
    )
    with source_connection() as connection:
        rows = fetch_all(connection, statement)
    return pd.DataFrame(rows)


def profile_numeric(
    dataset: str, column: str, series: pd.Series, bins: int, edges: list[float] | None = None
) -> ColumnProfile:
    """Summarise a numeric column, optionally against fixed bin edges."""
    numeric = pd.to_numeric(series, errors="coerce")
    null_count = int(numeric.isna().sum())
    values = numeric.dropna().astype(float)

    profile = ColumnProfile(
        dataset=dataset,
        column=column,
        kind="numeric",
        sample_size=int(len(values)),
        null_count=null_count,
        distinct_count=int(values.nunique()),
    )
    if values.empty:
        return profile

    profile.mean = float(values.mean())
    profile.stddev = float(values.std(ddof=1)) if len(values) > 1 else 0.0
    profile.minimum = float(values.min())
    profile.p25 = float(values.quantile(0.25))
    profile.median = float(values.median())
    profile.p75 = float(values.quantile(0.75))
    profile.maximum = float(values.max())

    if edges is None:
        # Quantile edges rather than equal width: a log-normal column would
        # put almost every row in the first equal-width bin, making PSI blind
        # to anything but extreme shifts.
        quantiles = np.linspace(0, 1, bins + 1)
        candidate = np.unique(np.quantile(values, quantiles))
        if len(candidate) < 2:
            candidate = np.array([profile.minimum, profile.minimum + 1.0])
        edges_array = candidate.astype(float)
    else:
        edges_array = np.array(edges, dtype=float)

    # Open-ended outer edges so values outside the reference range still land
    # in a bin instead of being silently dropped.
    counting_edges = edges_array.copy()
    counting_edges[0] = -np.inf
    counting_edges[-1] = np.inf
    counts, _ = np.histogram(values, bins=counting_edges)

    profile.bin_edges = [float(edge) for edge in edges_array]
    profile.bin_counts = [int(count) for count in counts]
    return profile


def profile_categorical(
    dataset: str, column: str, series: pd.Series, max_categories: int
) -> ColumnProfile:
    """Summarise a categorical column."""
    null_count = int(series.isna().sum())
    values = series.dropna().astype(str)
    counts = values.value_counts()

    if len(counts) > max_categories:
        # Keep the most common and pool the tail, so the profile stays bounded
        # no matter how many distinct values arrive.
        head = counts.iloc[:max_categories]
        pooled = int(counts.iloc[max_categories:].sum())
        category_counts = {str(k): int(v) for k, v in head.items()}
        category_counts[OTHER_BUCKET] = pooled
    else:
        category_counts = {str(k): int(v) for k, v in counts.items()}

    return ColumnProfile(
        dataset=dataset,
        column=column,
        kind="categorical",
        sample_size=int(len(values)),
        null_count=null_count,
        distinct_count=int(counts.size),
        category_counts=category_counts,
    )


def population_stability_index(
    reference_counts: list[int], current_counts: list[int]
) -> float:
    """PSI between two binned distributions sharing the same edges."""
    if len(reference_counts) != len(current_counts):
        raise ValueError("PSI needs the same number of bins on both sides")

    reference_total = sum(reference_counts) or 1
    current_total = sum(current_counts) or 1

    psi = 0.0
    for ref, cur in zip(reference_counts, current_counts, strict=True):
        expected = max(ref / reference_total, PSI_EPSILON)
        actual = max(cur / current_total, PSI_EPSILON)
        psi += (actual - expected) * math.log(actual / expected)
    return float(psi)


def compare_numeric(
    reference: ColumnProfile, current: ColumnProfile, spec: NumericDriftSpec, min_sample: int
) -> DriftResult:
    base = DriftResult(
        dataset=current.dataset,
        column=spec.column,
        kind="numeric",
        method="psi",
        reference_sample_size=reference.sample_size if reference else None,
        current_sample_size=current.sample_size,
        min_sample_size=min_sample,
        statistic=None,
        p_value=None,
        degrees_of_freedom=None,
        threshold_warning=spec.warning,
        threshold_critical=spec.critical,
        status="inconclusive",
        severity=Severity.INFO,
        is_significant=False,
        detail="",
    )

    if reference is None or not reference.bin_counts:
        base.detail = "no reference profile yet; drift cannot be computed on one run"
        return base

    if current.sample_size < min_sample or reference.sample_size < min_sample:
        base.detail = (
            f"sample too small: reference {reference.sample_size}, current "
            f"{current.sample_size}, minimum {min_sample}. The test was NOT run and "
            "no drift is claimed"
        )
        LOGGER.info("%s.%s: %s", current.dataset, spec.column, base.detail)
        return base

    psi = population_stability_index(reference.bin_counts, current.bin_counts or [])
    base.statistic = round(psi, 8)

    if psi >= spec.critical:
        base.status, base.severity, base.is_significant = "critical", Severity.CRITICAL, True
    elif psi >= spec.warning:
        base.status, base.severity, base.is_significant = "warning", Severity.WARNING, True
    else:
        base.status, base.severity = "ok", Severity.INFO

    base.detail = (
        f"PSI {psi:.4f} over {len(reference.bin_counts)} reference bins "
        f"(n_ref={reference.sample_size}, n_cur={current.sample_size}); "
        f"warning at {spec.warning}, critical at {spec.critical}. "
        f"Empty bins floored at {PSI_EPSILON}"
    )
    return base


def compare_categorical(
    reference: ColumnProfile, current: ColumnProfile, spec: CategoricalDriftSpec, min_sample: int
) -> DriftResult:
    from scipy import stats

    base = DriftResult(
        dataset=current.dataset,
        column=spec.column,
        kind="categorical",
        method="chi_square",
        reference_sample_size=reference.sample_size if reference else None,
        current_sample_size=current.sample_size,
        min_sample_size=min_sample,
        statistic=None,
        p_value=None,
        degrees_of_freedom=None,
        threshold_warning=spec.alpha,
        threshold_critical=spec.alpha,
        status="inconclusive",
        severity=Severity.INFO,
        is_significant=False,
        detail="",
    )

    if reference is None or not reference.category_counts:
        base.detail = "no reference profile yet; drift cannot be computed on one run"
        return base

    if current.sample_size < min_sample or reference.sample_size < min_sample:
        base.detail = (
            f"sample too small: reference {reference.sample_size}, current "
            f"{current.sample_size}, minimum {min_sample}. The test was NOT run and "
            "no drift is claimed"
        )
        LOGGER.info("%s.%s: %s", current.dataset, spec.column, base.detail)
        return base

    reference_counts = dict(reference.category_counts)
    current_counts = dict(current.category_counts or {})
    categories = sorted(set(reference_counts) | set(current_counts))

    reference_total = sum(reference_counts.values())
    current_total = sum(current_counts.values())
    if reference_total == 0 or current_total == 0:
        base.detail = (
            f"one side has no non-null values (n_ref={reference_total}, "
            f"n_cur={current_total}), so the mixes cannot be compared"
        )
        return base

    # Both sides are SAMPLES, so this is a test of homogeneity on a 2 x k
    # table, not a goodness-of-fit test against the reference proportions.
    #
    # Why that distinction matters (measured, not theoretical)
    # -------------------------------------------------------
    # The earlier implementation here took the reference proportions as a
    # KNOWN distribution, scaled them to the current row count and ran a
    # one-way chi-square. That ignores the sampling error in the reference
    # itself. On two genuinely identical samples of a uniform 20-category
    # column (n_ref=1000, n_cur=10000) it reported chi2=99.415 on 19 df,
    # p=6.8e-13 - a confident false positive produced entirely by the
    # reference's +/-7-count noise being multiplied by ten.
    #
    # The 2 x k homogeneity test on the same two stored profiles gives
    # chi2=9.012 on 19 df, p=0.973: no drift, which is the truth. Both tests
    # share the same k-1 degrees of freedom; the difference is only that this
    # one prices in the uncertainty on BOTH sides. A noisy reference
    # therefore widens the test rather than sharpening it, which is the
    # correct direction for a detector people are expected to trust.
    total = reference_total + current_total
    smaller_side = min(reference_total, current_total)

    # Pool categories whose smallest EXPECTED cell is too sparse for the
    # chi-square approximation. Under the homogeneity null the expected cell
    # for a category is row_total * column_total / grand_total, so the
    # smaller of the two cells is governed by the smaller sample.
    kept: list[str] = []
    pooled_reference = 0
    pooled_current = 0
    for category in categories:
        column_total = reference_counts.get(category, 0) + current_counts.get(category, 0)
        if smaller_side * column_total / total < spec.min_expected_frequency:
            pooled_reference += reference_counts.get(category, 0)
            pooled_current += current_counts.get(category, 0)
        else:
            kept.append(category)

    reference_row = [reference_counts.get(c, 0) for c in kept]
    current_row = [current_counts.get(c, 0) for c in kept]
    pooled_note = ""
    if pooled_reference + pooled_current > 0:
        reference_row.append(pooled_reference)
        current_row.append(pooled_current)
        pooled_note = (
            f"; {len(categories) - len(kept)} sparse categor"
            f"{'y' if len(categories) - len(kept) == 1 else 'ies'} pooled into "
            f"{OTHER_BUCKET} (expected cell < {spec.min_expected_frequency})"
        )

    # A category that is empty on BOTH sides carries no information and would
    # give a zero expected cell, so it is dropped rather than pooled.
    table = [
        (left, right)
        # strict: the two rows are built in lockstep from `kept` plus the one
        # optional pooled bucket, so a length mismatch is a bug in this
        # function and should raise rather than silently truncate the table.
        for left, right in zip(reference_row, current_row, strict=True)
        if left + right > 0
    ]
    if len(table) < 2:
        base.detail = (
            "fewer than two populated categories remain after pooling, so a "
            "chi-square test cannot be computed" + pooled_note
        )
        return base

    statistic, p_value, degrees_of_freedom, _expected = stats.chi2_contingency(
        [[left for left, _ in table], [right for _, right in table]],
        correction=False,
    )
    base.statistic = round(float(statistic), 8)
    base.p_value = round(float(p_value), 12)
    base.degrees_of_freedom = int(degrees_of_freedom)

    if p_value < spec.alpha:
        base.status = "warning"
        base.severity = Severity.WARNING
        base.is_significant = True
    else:
        base.status = "ok"
        base.severity = Severity.INFO

    base.detail = (
        f"chi-square homogeneity {statistic:.3f} on {base.degrees_of_freedom} df, "
        f"p={p_value:.2e} (alpha {spec.alpha}; n_ref={reference.sample_size}, "
        f"n_cur={current.sample_size}){pooled_note}"
    )
    return base


def build_profiles(contract: Contract, partition_date: date) -> list[ColumnProfile]:
    """Profile every column the contract names for drift monitoring."""
    spec = contract.distribution
    if not spec.enabled:
        return []

    numeric_columns = [item.column for item in spec.numeric]
    categorical_columns = [item.column for item in spec.categorical]
    wanted = numeric_columns + categorical_columns
    if not wanted:
        return []

    frame = _load_frame(contract, partition_date, wanted)
    if frame.empty:
        LOGGER.warning(
            "%s: no data for distribution profiling of %s", contract.dataset, wanted
        )
        return []

    profiles: list[ColumnProfile] = []
    for item in spec.numeric:
        if item.column not in frame.columns:
            LOGGER.warning("%s: column %s absent, cannot profile", contract.dataset, item.column)
            continue
        profiles.append(
            profile_numeric(contract.dataset, item.column, frame[item.column], item.bins)
        )
    for item in spec.categorical:
        if item.column not in frame.columns:
            LOGGER.warning("%s: column %s absent, cannot profile", contract.dataset, item.column)
            continue
        profiles.append(
            profile_categorical(
                contract.dataset, item.column, frame[item.column], item.max_categories
            )
        )
    return profiles


def rebin_against_reference(
    contract: Contract,
    partition_date: date,
    reference: ColumnProfile,
    spec: NumericDriftSpec,
) -> ColumnProfile:
    """Re-profile a numeric column using the reference bin edges.

    This is the step that makes PSI meaningful: both sides must be counted
    into the SAME buckets.
    """
    frame = _load_frame(contract, partition_date, [spec.column])
    if frame.empty or spec.column not in frame.columns:
        return ColumnProfile(
            dataset=contract.dataset,
            column=spec.column,
            kind="numeric",
            sample_size=0,
            null_count=0,
        )
    return profile_numeric(
        contract.dataset,
        spec.column,
        frame[spec.column],
        spec.bins,
        edges=reference.bin_edges,
    )
