"""The weighted 0-100 quality score.

Formula
-------
Each of the six dimensions gets a sub-score in 0-100. The overall score is the
weighted mean of the dimensions that were ACTUALLY MEASURED:

    overall = sum(score[d] * weight[d] for d in measured) / sum(weight[d] for d in measured)

Per-dimension sub-scores:

* completeness / uniqueness / validity / consistency
      From the contract rules of that dimension. A rule contributes its pass
      fraction (1 - unexpected/total) rather than a binary pass, so one bad
      row out of 100,000 does not read the same as half the table being wrong.
      Rules are weighted by their severity (CRITICAL 4, ERROR 3, WARNING 2,
      INFO 1), so failing a critical rule moves the score more than failing an
      advisory one.
* freshness
      100 when ok, a linear ramp between the warning and critical thresholds,
      0 at or beyond critical.
* volume
      100 when ok; derived from the robust z-score between the warning and
      critical bounds; 0 beyond critical.

How missing measurements are treated
------------------------------------
A dimension that produced NO measurement is excluded from the mean and the
remaining weights are renormalised. It is never scored as 100. Treating an
unmeasured dimension as perfect is the single most dangerous shortcut in a
quality score: it means a check that failed to run improves the score.
Unmeasured dimensions are listed on the row so a high score can always be
read together with what it did not cover.

The rule the score can never break
----------------------------------
`health_status` is NOT a pure function of `overall_score`. If any CRITICAL
contract rule failed, or a CRITICAL freshness/volume/schema finding exists,
the dataset is `unhealthy` however high the weighted score happens to be. A
dataset can be 97/100 and unhealthy - and that is the correct reading, because
"almost all the rows are fine" is no comfort when the ones that are not are
the financial totals. The database enforces this too, via
`ck_critical_is_not_healthy`.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field

from src.common.logging import get_logger
from src.common.severity import Severity
from src.contracts.models import DIMENSIONS, Contract
from src.observability.freshness import FreshnessResult
from src.observability.schema_tracker import SchemaChange
from src.observability.volume import VolumeResult
from src.validation.runner import RuleOutcome

LOGGER = get_logger(__name__)

# How much a rule counts towards its dimension, by declared severity.
SEVERITY_WEIGHTS: dict[Severity, float] = {
    Severity.CRITICAL: 4.0,
    Severity.ERROR: 3.0,
    Severity.WARNING: 2.0,
    Severity.INFO: 1.0,
}


@dataclass
class QualityScore:
    dataset: str
    overall_score: float
    dimension_scores: dict[str, float | None] = field(default_factory=dict)
    dimensions_measured: int = 0
    dimensions_total: int = len(DIMENSIONS)
    weights_used: dict[str, float] = field(default_factory=dict)
    unmeasured_dimensions: list[str] = field(default_factory=list)
    has_critical_failure: bool = False
    health_status: str = "unknown"
    detail: str = ""


def _rule_score(outcome: RuleOutcome) -> float:
    """A rule pass fraction in 0-1."""
    if outcome.success:
        return 1.0
    total = outcome.element_count
    unexpected = outcome.unexpected_count
    if total and unexpected is not None and total > 0:
        return max(0.0, 1.0 - (unexpected / total))
    # A SQL or pandas rule that failed without a usable ratio (or a rule that
    # could not run at all) counts as a full failure. Guessing a partial pass
    # would flatter a dataset whose check did not complete.
    return 0.0


def _dimension_score(outcomes: Iterable[RuleOutcome]) -> float | None:
    numerator = 0.0
    denominator = 0.0
    for outcome in outcomes:
        weight = SEVERITY_WEIGHTS[outcome.severity]
        numerator += _rule_score(outcome) * weight
        denominator += weight
    if denominator == 0:
        return None
    return round(numerator / denominator * 100.0, 2)


def _freshness_score(result: FreshnessResult | None) -> float | None:
    if result is None or result.status == "unknown":
        return None
    if result.lag_seconds is None:
        return None
    warn = result.warning_after_seconds
    crit = result.critical_after_seconds
    lag = result.lag_seconds
    if warn is None or crit is None or crit <= warn:
        return 100.0 if result.status == "ok" else 0.0
    if lag <= warn:
        return 100.0
    if lag >= crit:
        return 0.0
    # Linear ramp across the band, so a dataset drifting towards critical is
    # visible before it crosses the line.
    return round((1.0 - (lag - warn) / (crit - warn)) * 100.0, 2)


def _volume_score(result: VolumeResult | None) -> float | None:
    if result is None or result.status == "unknown":
        return None
    z = abs(result.robust_z_score or 0.0)
    warn = 3.5
    crit = 6.0
    if result.status == "ok" and result.robust_z_score is None:
        return 100.0
    if z <= warn:
        return 100.0
    if z >= crit:
        return 0.0
    return round((1.0 - (z - warn) / (crit - warn)) * 100.0, 2)


def compute(
    contract: Contract,
    *,
    rule_outcomes: list[RuleOutcome],
    freshness_result: FreshnessResult | None = None,
    volume_result: VolumeResult | None = None,
    schema_changes: list[SchemaChange] | None = None,
) -> QualityScore:
    """Compute the score for one dataset from everything measured this run."""
    by_dimension: dict[str, list[RuleOutcome]] = {}
    for outcome in rule_outcomes:
        by_dimension.setdefault(outcome.dimension, []).append(outcome)

    scores: dict[str, float | None] = {}
    for dimension in ("completeness", "uniqueness", "validity", "consistency"):
        scores[dimension] = _dimension_score(by_dimension.get(dimension, []))
    scores["freshness"] = _freshness_score(freshness_result)
    scores["volume"] = _volume_score(volume_result)

    measured = {name for name, value in scores.items() if value is not None}
    unmeasured = sorted(set(DIMENSIONS) - measured)
    weights = contract.score.normalised_weights(measured)

    if weights:
        overall = sum(scores[name] * weight for name, weight in weights.items())
        overall = round(min(max(overall, 0.0), 100.0), 2)
    else:
        # Nothing was measured at all. The score is 0 and the status unknown -
        # not 100, which is what an empty weighted mean would otherwise imply.
        overall = 0.0

    # --- the rule the score can never break ------------------------------
    critical_rule_failed = any(
        not outcome.success and outcome.severity is Severity.CRITICAL
        for outcome in rule_outcomes
    )
    critical_freshness = (
        freshness_result is not None and freshness_result.severity is Severity.CRITICAL
    )
    critical_volume = (
        volume_result is not None and volume_result.severity is Severity.CRITICAL
    )
    critical_schema = any(
        change.severity is Severity.CRITICAL for change in (schema_changes or [])
    )
    has_critical = bool(
        critical_rule_failed or critical_freshness or critical_volume or critical_schema
    )

    if not measured:
        health = "unknown"
    elif has_critical:
        # Deliberately independent of the score. A critical contract breach
        # cannot be averaged away by a hundred passing rules.
        health = "unhealthy"
    elif overall >= contract.score.healthy_at:
        health = "healthy"
    elif overall >= contract.score.degraded_at:
        health = "degraded"
    else:
        health = "unhealthy"

    sources = []
    if critical_rule_failed:
        sources.append("a CRITICAL contract rule failed")
    if critical_freshness:
        sources.append("freshness is CRITICAL")
    if critical_volume:
        sources.append("volume is CRITICAL")
    if critical_schema:
        sources.append("a CRITICAL schema change was detected")

    detail = f"{overall:.2f}/100 over {len(measured)} of {len(DIMENSIONS)} dimensions"
    if unmeasured:
        detail += f"; not measured: {', '.join(unmeasured)} (excluded, not scored as 100)"
    if has_critical:
        detail += f"; forced unhealthy because {', and '.join(sources)}"

    result = QualityScore(
        dataset=contract.dataset,
        overall_score=overall,
        dimension_scores=scores,
        dimensions_measured=len(measured),
        dimensions_total=len(DIMENSIONS),
        weights_used={name: round(weight, 6) for name, weight in weights.items()},
        unmeasured_dimensions=unmeasured,
        has_critical_failure=has_critical,
        health_status=health,
        detail=detail,
    )
    LOGGER.info("%s score %s -> %s (%s)", contract.dataset, overall, health, detail)
    return result
