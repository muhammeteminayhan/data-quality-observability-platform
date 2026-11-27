"""Turn detector measurements into findings, and findings into incidents.

This is the layer that decides what counts as an anomaly worth telling someone
about. The detectors measure; this module judges.

Two rules shape it.

**An anomaly needs a measurement.** Every finding here is derived from a
detector result that actually ran. A detector that returned `unknown` produces
a finding only when the contract asked for one (for example a missing
timestamp at WARNING), and that finding says explicitly that the dataset could
not be measured. Nothing is ever inferred from an absent signal.

**A passing measurement is itself evidence.** When a detector ran and passed,
this module calls incidents.record_healthy for the matching fingerprint. That
is the only route to resolving an incident, and it is why resolution cannot
happen by silence: no measurement, no healthy check, no resolution.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field

import psycopg

from src.common.logging import get_logger
from src.common.severity import Severity
from src.contracts.models import Contract
from src.observability import incidents
from src.observability.distribution import DriftResult
from src.observability.freshness import FreshnessResult
from src.observability.incidents import Finding, IncidentAction
from src.observability.quality_score import QualityScore
from src.observability.schema_tracker import SchemaChange
from src.observability.volume import VolumeResult
from src.validation.runner import RuleOutcome

LOGGER = get_logger(__name__)


@dataclass
class Reconciliation:
    """What the incident layer did for one dataset this run."""

    dataset: str
    findings: list[Finding] = field(default_factory=list)
    actions: list[IncidentAction] = field(default_factory=list)
    healthy_checks: int = 0
    resolved: list[IncidentAction] = field(default_factory=list)

    @property
    def alertable(self) -> list[IncidentAction]:
        return [action for action in self.actions if action.should_alert]


def findings_from_validation(
    contract: Contract, outcomes: list[RuleOutcome]
) -> list[Finding]:
    """One finding per FAILING rule. Passing rules produce healthy checks."""
    found: list[Finding] = []
    for outcome in outcomes:
        if outcome.success:
            continue
        if outcome.unexpected_count is not None and outcome.element_count:
            observed = f"{outcome.unexpected_count} of {outcome.element_count} rows"
        else:
            observed = outcome.observed_value
        target = outcome.column or "the table"
        description = outcome.exception_message or (
            f"{outcome.expectation_type} failed on {target}; {observed} unexpected"
        )
        found.append(
            Finding(
                dataset=contract.dataset,
                detector="validation",
                rule_name=outcome.rule_name,
                dimension=outcome.dimension,
                title=f"{contract.dataset}: rule {outcome.rule_name} failed",
                description=description,
                severity=outcome.severity,
                observed_value=observed,
                expected_value=outcome.expected_value,
            )
        )
    return found


def findings_from_freshness(
    contract: Contract, result: FreshnessResult
) -> list[Finding]:
    if result.status == "ok":
        return []
    if result.status == "unknown" and result.severity is Severity.INFO:
        # Freshness is disabled for this dataset; nothing to report.
        return []
    return [
        Finding(
            dataset=contract.dataset,
            detector="freshness",
            rule_name="freshness",
            dimension="freshness",
            title=f"{contract.dataset}: data is {result.status}",
            description=result.detail,
            severity=result.severity,
            observed_value=result.lag_seconds,
            expected_value=result.warning_after_seconds,
        )
    ]


def findings_from_volume(contract: Contract, result: VolumeResult) -> list[Finding]:
    if result.status in ("ok", "unknown"):
        # `unknown` means the baseline could not be computed. That is not an
        # anomaly - claiming one from insufficient history is exactly the
        # false alarm the volume module refuses to raise.
        return []
    return [
        Finding(
            dataset=contract.dataset,
            detector="volume",
            rule_name="volume",
            dimension="volume",
            title=f"{contract.dataset}: row count is {result.status}",
            description=result.detail,
            severity=result.severity,
            observed_value=result.row_count,
            expected_value=result.baseline_median,
        )
    ]


def findings_from_schema(
    contract: Contract, changes: list[SchemaChange]
) -> list[Finding]:
    found: list[Finding] = []
    for change in changes:
        if change.severity is Severity.INFO:
            # An additive change is recorded in schema_changes but does not
            # open an incident: a new nullable column breaks nothing.
            continue
        found.append(
            Finding(
                dataset=contract.dataset,
                detector="schema",
                rule_name=f"schema:{change.change_type}:{change.column_name}",
                dimension="validity",
                title=f"{contract.dataset}: {change.change_type} on {change.column_name}",
                description=change.detail,
                severity=change.severity,
                observed_value=change.new_type,
                expected_value=change.previous_type,
            )
        )
    return found


def findings_from_drift(contract: Contract, results: list[DriftResult]) -> list[Finding]:
    found: list[Finding] = []
    for result in results:
        # `inconclusive` never produces a finding: the sample was too small,
        # so there is nothing to claim. `is_significant` is also required, so
        # a statistic that did not cross a threshold stays silent.
        if result.status in ("ok", "inconclusive") or not result.is_significant:
            continue
        found.append(
            Finding(
                dataset=contract.dataset,
                detector="distribution",
                rule_name=f"drift:{result.column}",
                dimension="validity",
                title=f"{contract.dataset}: {result.column} distribution shifted",
                description=result.detail,
                severity=result.severity,
                observed_value=result.statistic,
                expected_value=result.threshold_warning,
            )
        )
    return found


def findings_from_score(contract: Contract, score: QualityScore) -> list[Finding]:
    """A finding for the score itself only when the dataset is unhealthy.

    Deliberately NOT raised for `degraded`: the underlying detectors have
    already opened incidents for whatever caused the drop, and a second
    incident for the aggregate would double-count one problem.
    """
    if score.health_status != "unhealthy":
        return []
    severity = Severity.CRITICAL if score.has_critical_failure else Severity.ERROR
    return [
        Finding(
            dataset=contract.dataset,
            detector="quality_score",
            rule_name="quality_score",
            dimension="validity",
            title=f"{contract.dataset}: quality score {score.overall_score}",
            description=score.detail,
            severity=severity,
            observed_value=score.overall_score,
            expected_value=contract.score.healthy_at,
        )
    ]


def _detectors_that_ran(
    *,
    rule_outcomes: list[RuleOutcome],
    freshness_result: FreshnessResult | None,
    volume_result: VolumeResult | None,
    schema_changes: list[SchemaChange] | None,
    drift_results: list[DriftResult] | None,
    score: QualityScore | None,
) -> set[str]:
    """Which detectors produced a real measurement this run.

    This is the gate on resolution. A detector that did not run, or that ran
    and could not conclude, is absent from this set - so an incident it owns
    cannot be resolved by this run. That is the difference between
    "we checked and it is fine" and "we did not check".

    Specifically excluded:
      * freshness / volume with status `unknown` - no usable timestamp, or not
        enough history for a baseline;
      * distribution where every result was `inconclusive` - the samples were
        below the contract minimum, so nothing was actually tested;
      * quality_score with health `unknown` - nothing was measured at all.
    """
    ran: set[str] = set()
    if rule_outcomes:
        ran.add("validation")
    if freshness_result is not None and freshness_result.status != "unknown":
        ran.add("freshness")
    if volume_result is not None and volume_result.status != "unknown":
        ran.add("volume")
    if schema_changes is not None:
        # The schema is always observable when a snapshot was taken, so an
        # empty change list is a genuine "nothing changed" measurement.
        ran.add("schema")
    if drift_results is not None and any(
        result.status != "inconclusive" for result in drift_results
    ):
        ran.add("distribution")
    if score is not None and score.health_status != "unknown":
        ran.add("quality_score")
    return ran


def reconcile(
    connection: psycopg.Connection,
    contract: Contract,
    *,
    run_id: uuid.UUID,
    dataset_id: int,
    rule_outcomes: list[RuleOutcome],
    freshness_result: FreshnessResult | None = None,
    volume_result: VolumeResult | None = None,
    schema_changes: list[SchemaChange] | None = None,
    drift_results: list[DriftResult] | None = None,
    score: QualityScore | None = None,
) -> Reconciliation:
    """Open, update and resolve incidents from one run of the detectors."""
    result = Reconciliation(dataset=contract.dataset)

    result.findings.extend(findings_from_validation(contract, rule_outcomes))
    if freshness_result is not None:
        result.findings.extend(findings_from_freshness(contract, freshness_result))
    if volume_result is not None:
        result.findings.extend(findings_from_volume(contract, volume_result))
    if schema_changes:
        result.findings.extend(findings_from_schema(contract, schema_changes))
    if drift_results:
        result.findings.extend(findings_from_drift(contract, drift_results))
    if score is not None:
        result.findings.extend(findings_from_score(contract, score))

    for finding in result.findings:
        result.actions.append(
            incidents.open_or_update(
                connection, finding, run_id=run_id, dataset_id=dataset_id
            )
        )

    # Positive evidence.
    #
    # Resolution starts from the OPEN incidents rather than from this run's
    # passing checks, because some detectors key their findings on the
    # specific thing that broke (a schema change on one column, drift on one
    # column). The only way to say "that specific problem is gone" is to take
    # each live incident, confirm its detector ran, and confirm its finding is
    # absent now.
    #
    # An incident whose detector did NOT run this time is left strictly alone:
    # it gets no healthy check, so it cannot drift towards resolution while
    # nobody is looking.
    ran = _detectors_that_ran(
        rule_outcomes=rule_outcomes,
        freshness_result=freshness_result,
        volume_result=volume_result,
        schema_changes=schema_changes,
        drift_results=drift_results,
        score=score,
    )
    failing_fingerprints = {finding.fingerprint for finding in result.findings}

    for live in incidents.live_for_dataset(connection, dataset_id):
        detector = live["detector"]
        if detector not in ran:
            LOGGER.debug(
                "incident %s: detector %s did not run, no healthy check recorded",
                live["incident_id"],
                detector,
            )
            continue
        if live["fingerprint"] in failing_fingerprints:
            continue
        result.healthy_checks += 1
        resolved = incidents.record_healthy_by_fingerprint(
            connection,
            incident_id=int(live["incident_id"]),
            fingerprint=live["fingerprint"],
            dataset=contract.dataset,
            run_id=run_id,
            detail=(
                f"{detector} ran for {contract.dataset} and the finding "
                f"{live['rule_name']} is no longer present: a positive "
                "re-measurement, not an absence of alerts"
            ),
        )
        if resolved is not None:
            result.resolved.append(resolved)

    LOGGER.info(
        "%s: %d finding(s), %d incident action(s), %d healthy check(s), %d resolved",
        contract.dataset,
        len(result.findings),
        len(result.actions),
        result.healthy_checks,
        len(result.resolved),
    )
    return result
