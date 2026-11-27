"""Typed representation of a data contract.

The YAML is parsed into these dataclasses once, validated at the boundary, and
everything downstream works with the typed objects. A malformed contract fails
at load time with the file name and the offending key, rather than producing a
check that silently does nothing.

Nothing in this module knows the name of any dataset, column or rule.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

from src.common.severity import Severity

# The six quality dimensions the score is built from.
Dimension = Literal[
    "completeness", "uniqueness", "validity", "consistency", "freshness", "volume"
]

DIMENSIONS: tuple[str, ...] = (
    "completeness",
    "uniqueness",
    "validity",
    "consistency",
    "freshness",
    "volume",
)


class ContractError(ValueError):
    """Raised when a contract file cannot be understood."""


@dataclass(frozen=True)
class SourceSpec:
    """Where the dataset physically lives."""

    type: Literal["postgres", "parquet"]
    location: str
    # Preferred timestamp for freshness: an event time if the dataset has one.
    event_timestamp_column: str | None = None
    # Fallback: when the row was ingested.
    ingestion_timestamp_column: str | None = None
    # Parquet only: the partition column used to address one day.
    partition_column: str | None = None


@dataclass(frozen=True)
class FreshnessSpec:
    enabled: bool = True
    warning_after_minutes: int | None = None
    critical_after_minutes: int | None = None
    # What to report when the dataset exposes no usable timestamp at all.
    # Never "ok": an unmeasurable dataset is unknown, not fresh.
    on_missing_timestamp: Severity = Severity.WARNING

    def thresholds_seconds(self) -> tuple[int | None, int | None]:
        warn = self.warning_after_minutes * 60 if self.warning_after_minutes else None
        crit = self.critical_after_minutes * 60 if self.critical_after_minutes else None
        return warn, crit


@dataclass(frozen=True)
class VolumeBaselineSpec:
    """How the expected row count is derived from history."""

    method: Literal["rolling_median_mad", "absolute_bounds"] = "rolling_median_mad"
    window_days: int = 28
    # Below this many comparable history points the baseline is not trusted and
    # the check reports `unknown` rather than inventing a threshold.
    min_history_points: int = 5
    warning_z: float = 3.5
    critical_z: float = 6.0
    # Compare weekends against weekends and weekdays against weekdays. Without
    # this a normal Sunday dip looks like a 40 percent volume loss.
    same_day_type: bool = True


@dataclass(frozen=True)
class VolumeSpec:
    enabled: bool = True
    min_rows: int | None = None
    max_rows: int | None = None
    severity: Severity = Severity.ERROR
    baseline: VolumeBaselineSpec = field(default_factory=VolumeBaselineSpec)


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    type: str
    nullable: bool = True
    description: str | None = None


@dataclass(frozen=True)
class SchemaSpec:
    enforce: bool = True
    columns: tuple[ColumnSpec, ...] = ()
    # Compare the contract nullable flag against the PHYSICAL column
    # definition. Off by default and on purpose: in this platform the raw
    # layer carries no NOT NULL constraints at all, so that a corrupt row can
    # be ingested and then detected. A contract saying `nullable: false` is a
    # statement about the DATA, enforced by the completeness rules, not about
    # the DDL. Comparing the two would report the platform own design as a
    # BREAKING schema change on every single run.
    check_nullability: bool = False
    # Severity per classification. A new nullable column is routine; losing a
    # column the contract requires is not.
    on_additive: Severity = Severity.INFO
    on_type_change: Severity = Severity.CRITICAL
    on_column_removal: Severity = Severity.CRITICAL
    on_rename_candidate: Severity = Severity.ERROR
    on_breaking: Severity = Severity.CRITICAL

    def column(self, name: str) -> ColumnSpec | None:
        for spec in self.columns:
            if spec.name == name:
                return spec
        return None

    @property
    def required_columns(self) -> tuple[str, ...]:
        return tuple(spec.name for spec in self.columns)

    def severity_for(self, change_type: str) -> Severity:
        mapping = {
            "ADDITIVE": self.on_additive,
            "TYPE_CHANGE": self.on_type_change,
            "COLUMN_REMOVAL": self.on_column_removal,
            "COLUMN_RENAME_CANDIDATE": self.on_rename_candidate,
            "BREAKING": self.on_breaking,
        }
        try:
            return mapping[change_type]
        except KeyError as exc:
            raise ContractError(f"unknown schema change type {change_type!r}") from exc


@dataclass(frozen=True)
class NumericDriftSpec:
    column: str
    method: Literal["psi"] = "psi"
    bins: int = 10
    warning: float = 0.10
    critical: float = 0.25


@dataclass(frozen=True)
class CategoricalDriftSpec:
    column: str
    method: Literal["chi_square"] = "chi_square"
    # Significance level. A p-value below this is evidence of a shift.
    alpha: float = 0.01
    # Chi-square is unreliable when expected cell counts are tiny, so a
    # category below this expected frequency is pooled into "other".
    min_expected_frequency: int = 5
    max_categories: int = 20


@dataclass(frozen=True)
class DistributionSpec:
    enabled: bool = False
    # Below this many rows on either side the test is not run at all and the
    # result is `inconclusive`. Claiming drift from a tiny sample is the
    # single easiest way to make a drift detector useless.
    min_sample_size: int = 500
    severity: Severity = Severity.WARNING
    numeric: tuple[NumericDriftSpec, ...] = ()
    categorical: tuple[CategoricalDriftSpec, ...] = ()


@dataclass(frozen=True)
class QualityRule:
    """One assertion about the data."""

    name: str
    dimension: str
    severity: Severity
    expectation: str
    column: str | None = None
    # Free-form arguments passed straight to the expectation, so a new GX
    # expectation needs no code change here.
    kwargs: dict[str, Any] = field(default_factory=dict)
    # For SQL-based rules (referential integrity, business consistency).
    sql: str | None = None
    description: str | None = None
    # Tolerated failure fraction. A rule can legitimately allow a small
    # percentage - e.g. a free-text column that is occasionally blank.
    max_unexpected_percent: float = 0.0

    @property
    def is_sql_rule(self) -> bool:
        return self.sql is not None


@dataclass(frozen=True)
class ScoreSpec:
    """Weights for the 0-100 quality score."""

    weights: dict[str, float] = field(
        default_factory=lambda: {
            "completeness": 0.25,
            "uniqueness": 0.15,
            "validity": 0.20,
            "consistency": 0.20,
            "freshness": 0.10,
            "volume": 0.10,
        }
    )
    # Score at or above which a dataset counts as healthy, provided nothing
    # critical failed.
    healthy_at: float = 90.0
    degraded_at: float = 70.0

    def normalised_weights(self, measured: set[str]) -> dict[str, float]:
        """Weights restricted to the dimensions that were measured.

        A dimension with no measurement is excluded and the remaining weights
        are renormalised to sum to 1. The alternative - treating an unmeasured
        dimension as 100 - would let a dataset score well precisely because a
        check failed to run.
        """
        active = {dim: w for dim, w in self.weights.items() if dim in measured and w > 0}
        total = sum(active.values())
        if total <= 0:
            return {}
        return {dim: weight / total for dim, weight in active.items()}


@dataclass(frozen=True)
class Contract:
    """A complete data contract."""

    dataset: str
    version: str
    source: SourceSpec
    owner: str | None = None
    criticality: Literal["low", "medium", "high", "critical"] = "medium"
    layer: Literal["raw", "staging", "analytics", "lake"] = "raw"
    description: str | None = None
    freshness: FreshnessSpec = field(default_factory=FreshnessSpec)
    volume: VolumeSpec = field(default_factory=VolumeSpec)
    schema: SchemaSpec = field(default_factory=SchemaSpec)
    distribution: DistributionSpec = field(default_factory=DistributionSpec)
    quality_rules: tuple[QualityRule, ...] = ()
    score: ScoreSpec = field(default_factory=ScoreSpec)
    # Set by the loader.
    content_sha256: str = ""
    source_path: str = ""
    raw: dict[str, Any] = field(default_factory=dict)

    # -- helpers ----------------------------------------------------------
    def rules_for_dimension(self, dimension: str) -> tuple[QualityRule, ...]:
        return tuple(rule for rule in self.quality_rules if rule.dimension == dimension)

    @property
    def declared_dimensions(self) -> set[str]:
        """Dimensions this contract actually says something about."""
        dims = {rule.dimension for rule in self.quality_rules}
        if self.freshness.enabled:
            dims.add("freshness")
        if self.volume.enabled:
            dims.add("volume")
        return dims

    @property
    def has_critical_rules(self) -> bool:
        return any(rule.severity is Severity.CRITICAL for rule in self.quality_rules)

    def rule(self, name: str) -> QualityRule | None:
        for candidate in self.quality_rules:
            if candidate.name == name:
                return candidate
        return None
