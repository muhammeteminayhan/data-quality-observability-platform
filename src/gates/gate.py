"""The quality gate: the one place that decides whether work continues.

Decision table
--------------
    CRITICAL present                      -> FAIL
    ERROR present                          -> FAIL for a blocking dataset,
                                              otherwise PASS_WITH_WARNINGS
    WARNING present, nothing worse         -> PASS_WITH_WARNINGS
    nothing above INFO                     -> PASS

A FAIL raises. That is the point: the specification is explicit that a failed
quality check must not surface as a successful pipeline run, so the gate turns
a CRITICAL finding into a failed Airflow task rather than a log line. The
decision is written to `gate_decisions` BEFORE the exception is raised, so the
record of why the pipeline stopped survives the failure.

`ck_critical_fails_gate` in the schema refuses a row where critical_count > 0
and decision = 'PASS', so even a future bug here cannot record a clean pass
over a critical finding.

Which datasets block
--------------------
From configuration (`DQ_BLOCKING_DATASETS`, surfaced as the Airflow variable
`dq_blocking_datasets`), not from a constant in the code. An ERROR on a
reference table should not stop the financial pipeline; an ERROR on
raw.payments should.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field

from src.common.config import get_settings
from src.common.db import insert_returning_id, observability_connection
from src.common.logging import get_logger
from src.common.severity import Severity, max_severity

LOGGER = get_logger(__name__)


class QualityGateFailed(RuntimeError):
    """Raised when the gate decides the pipeline must not continue."""

    def __init__(self, decision: GateDecision) -> None:
        super().__init__(decision.reason)
        self.decision = decision


@dataclass
class DatasetSignal:
    """What one dataset contributed to the gate."""

    dataset: str
    max_severity: Severity
    failed_rules: list[str] = field(default_factory=list)
    detail: str = ""


@dataclass
class GateDecision:
    gate_name: str
    decision: str  # PASS | PASS_WITH_WARNINGS | FAIL
    max_severity: Severity
    info_count: int = 0
    warning_count: int = 0
    error_count: int = 0
    critical_count: int = 0
    blocked_tasks: list[str] = field(default_factory=list)
    reason: str = ""
    signals: list[DatasetSignal] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return self.decision != "FAIL"


def evaluate(
    signals: list[DatasetSignal],
    *,
    gate_name: str = "data_quality_gate",
    blocking_datasets: tuple[str, ...] | None = None,
    downstream_tasks: list[str] | None = None,
) -> GateDecision:
    """Decide, without touching the database."""
    blocking = (
        blocking_datasets
        if blocking_datasets is not None
        else get_settings().blocking_datasets
    )

    counts = {Severity.INFO: 0, Severity.WARNING: 0, Severity.ERROR: 0, Severity.CRITICAL: 0}
    for signal in signals:
        counts[signal.max_severity] += 1

    worst = max_severity(signal.max_severity for signal in signals)

    critical = [s for s in signals if s.max_severity is Severity.CRITICAL]
    errors_on_blocking = [
        s
        for s in signals
        if s.max_severity is Severity.ERROR and s.dataset in blocking
    ]

    if critical:
        decision = "FAIL"
        names = ", ".join(sorted(s.dataset for s in critical))
        reason = (
            f"CRITICAL findings on {names}. A CRITICAL breach always fails the gate, "
            "so the pipeline stops rather than publishing data the contracts reject."
        )
    elif errors_on_blocking:
        decision = "FAIL"
        names = ", ".join(sorted(s.dataset for s in errors_on_blocking))
        reason = (
            f"ERROR findings on blocking dataset(s) {names}. These datasets are "
            "configured as blocking, so an ERROR stops the pipeline."
        )
    elif worst > Severity.INFO:
        decision = "PASS_WITH_WARNINGS"
        affected = ", ".join(
            sorted(s.dataset for s in signals if s.max_severity > Severity.INFO)
        )
        reason = (
            f"highest severity is {worst.value} on {affected}; the contracts tolerate "
            "this, so downstream work continues and the findings are recorded"
        )
    else:
        decision = "PASS"
        reason = f"all {len(signals)} dataset(s) within contract"

    return GateDecision(
        gate_name=gate_name,
        decision=decision,
        max_severity=worst,
        info_count=counts[Severity.INFO],
        warning_count=counts[Severity.WARNING],
        error_count=counts[Severity.ERROR],
        critical_count=counts[Severity.CRITICAL],
        blocked_tasks=list(downstream_tasks or []) if decision == "FAIL" else [],
        reason=reason,
        signals=signals,
    )


def record(decision: GateDecision, *, run_id: uuid.UUID) -> int:
    """Write the decision to gate_decisions and return its id."""
    with observability_connection() as connection:
        gate_decision_id = int(
            insert_returning_id(
                connection,
                """
                INSERT INTO gate_decisions (
                    run_id, gate_name, decision, max_severity,
                    info_count, warning_count, error_count, critical_count,
                    blocked_tasks, reason
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING gate_decision_id
                """,
                (
                    run_id,
                    decision.gate_name,
                    decision.decision,
                    decision.max_severity.value,
                    decision.info_count,
                    decision.warning_count,
                    decision.error_count,
                    decision.critical_count,
                    json.dumps(decision.blocked_tasks),
                    decision.reason,
                ),
            )
        )
    LOGGER.info(
        "gate %s -> %s (%s): %s",
        decision.gate_name,
        decision.decision,
        decision.max_severity.value,
        decision.reason,
    )
    return gate_decision_id


def enforce(
    signals: list[DatasetSignal],
    *,
    run_id: uuid.UUID,
    gate_name: str = "data_quality_gate",
    blocking_datasets: tuple[str, ...] | None = None,
    downstream_tasks: list[str] | None = None,
) -> GateDecision:
    """Evaluate, record, and raise on FAIL.

    Recording happens before the raise so the reason the pipeline stopped is
    queryable even though the task ends in failure. An Airflow task that calls
    this therefore turns a CRITICAL data-quality finding into a red task,
    which is the only honest way to show it.
    """
    decision = evaluate(
        signals,
        gate_name=gate_name,
        blocking_datasets=blocking_datasets,
        downstream_tasks=downstream_tasks,
    )
    record(decision, run_id=run_id)

    if not decision.passed:
        for signal in decision.signals:
            if signal.max_severity >= Severity.ERROR:
                LOGGER.error(
                    "gate blocker: %s [%s] %s",
                    signal.dataset,
                    signal.max_severity.value,
                    ", ".join(signal.failed_rules) or signal.detail,
                )
        raise QualityGateFailed(decision)

    return decision


def signal_from_parts(
    dataset: str,
    severities: list[Severity],
    *,
    failed_rules: list[str] | None = None,
    detail: str = "",
) -> DatasetSignal:
    """Build one dataset signal from every severity observed for it."""
    return DatasetSignal(
        dataset=dataset,
        max_severity=max_severity(severities),
        failed_rules=list(failed_rules or []),
        detail=detail,
    )
