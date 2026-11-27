"""Run contract validations through Great Expectations checkpoints.

Flow, per dataset:

    contract -> Expectation Suite -> Validation Definition -> Checkpoint
             -> CheckpointResult   -> RuleOutcome per rule

The Checkpoint is used rather than calling ValidationDefinition.run directly
because the checkpoint is where result_format and actions belong, and it is
the unit the specification names.

Results are persisted by src/validation/persist.py from the returned
CheckpointResult rather than by a GX action. The bundled GX actions are
notification and data-docs oriented; writing to observability_db from the
result keeps the schema and the transaction boundary under this platform's
control, and means a persistence failure surfaces as a task error instead of
being swallowed inside the action runner.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import date
from typing import Any

from great_expectations.checkpoint import Checkpoint
from great_expectations.core import ExpectationSuite
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.core.validation_definition import ValidationDefinition

from src.common.logging import get_logger
from src.common.severity import Severity, max_severity
from src.contracts.models import Contract
from src.validation.gx_context import (
    get_context,
    parquet_batch_definition,
    postgres_batch_definition,
)
from src.validation.suite_builder import build_suite, suite_name_for

LOGGER = get_logger(__name__)

# Bounded sample of offending values kept per failing rule. The point is to
# make a failure diagnosable, not to copy source data into the platform.
PARTIAL_UNEXPECTED_COUNT = 10


@dataclass
class RuleOutcome:
    """The result of one contract rule."""

    rule_name: str
    dimension: str
    severity: Severity
    expectation_type: str
    column: str | None
    success: bool
    element_count: int | None = None
    unexpected_count: int | None = None
    unexpected_percent: float | None = None
    observed_value: Any = None
    expected_value: Any = None
    partial_unexpected: list[Any] = field(default_factory=list)
    kwargs: dict[str, Any] = field(default_factory=dict)
    exception_message: str | None = None

    @property
    def effective_severity(self) -> Severity:
        """A passing rule contributes INFO, not its declared severity.

        Severity describes how bad a FAILURE is. Reporting a passing CRITICAL
        rule as CRITICAL would make every healthy run look like an outage.
        """
        return self.severity if not self.success else Severity.INFO


@dataclass
class ValidationOutcome:
    """Everything one dataset validation produced."""

    dataset: str
    suite_name: str
    engine: str = "great_expectations"
    engine_version: str | None = None
    checkpoint_name: str | None = None
    batch_identifier: str | None = None
    success: bool = True
    duration_ms: int = 0
    rows_evaluated: int | None = None
    error_message: str | None = None
    outcomes: list[RuleOutcome] = field(default_factory=list)

    @property
    def failed(self) -> list[RuleOutcome]:
        return [outcome for outcome in self.outcomes if not outcome.success]

    @property
    def passed_count(self) -> int:
        return sum(1 for outcome in self.outcomes if outcome.success)

    @property
    def max_severity(self) -> Severity:
        """Highest severity among the FAILING rules."""
        return max_severity(outcome.severity for outcome in self.failed)

    @property
    def has_critical_failure(self) -> bool:
        return any(outcome.severity is Severity.CRITICAL for outcome in self.failed)

    def by_dimension(self) -> dict[str, list[RuleOutcome]]:
        grouped: dict[str, list[RuleOutcome]] = {}
        for outcome in self.outcomes:
            grouped.setdefault(outcome.dimension, []).append(outcome)
        return grouped


def _batch_definition(contract: Contract, partition_date: date) -> Any:
    if contract.source.type == "postgres":
        return postgres_batch_definition(contract.source.location)
    return parquet_batch_definition(contract.source.location, partition_date.isoformat())


def _outcome_from_result(result: dict[str, Any]) -> RuleOutcome:
    """Map one GX expectation result onto the contract rule that produced it."""
    config = result.get("expectation_config") or {}
    kwargs = dict(config.get("kwargs") or {})
    meta = dict(config.get("meta") or {})
    detail = result.get("result") or {}

    # A GX result carries no contract knowledge of its own; the meta block
    # attached by the suite builder is the only link back to the rule.
    severity = Severity.parse(meta.get("severity", "ERROR"))
    exception_info = result.get("exception_info") or {}
    message = exception_info.get("exception_message") if exception_info else None

    return RuleOutcome(
        rule_name=meta.get("rule_name") or config.get("type") or "unknown",
        dimension=meta.get("dimension", "validity"),
        severity=severity,
        expectation_type=config.get("type", "unknown"),
        column=kwargs.get("column"),
        success=bool(result.get("success")),
        element_count=detail.get("element_count"),
        unexpected_count=detail.get("unexpected_count"),
        unexpected_percent=detail.get("unexpected_percent"),
        observed_value=detail.get("observed_value"),
        expected_value=kwargs.get("value_set") or kwargs.get("value"),
        partial_unexpected=list(detail.get("partial_unexpected_list") or [])[
            :PARTIAL_UNEXPECTED_COUNT
        ],
        kwargs={k: v for k, v in kwargs.items() if k != "batch_id"},
        exception_message=message,
    )


def validate_contract(
    contract: Contract,
    *,
    partition_date: date,
    run_name: str | None = None,
) -> ValidationOutcome:
    """Validate one dataset against its contract."""
    import great_expectations as gx

    context = get_context()
    suite_name = suite_name_for(contract)
    started = time.perf_counter()

    outcome = ValidationOutcome(
        dataset=contract.dataset,
        suite_name=suite_name,
        engine_version=gx.__version__,
    )

    try:
        suite: ExpectationSuite = context.suites.add_or_update(build_suite(contract))
        batch_definition = _batch_definition(contract, partition_date)

        validation_definition = context.validation_definitions.add_or_update(
            ValidationDefinition(
                name=f"{contract.dataset}__validation",
                data=batch_definition,
                suite=suite,
            )
        )

        checkpoint_name = f"{contract.dataset}__checkpoint"
        checkpoint = context.checkpoints.add_or_update(
            Checkpoint(
                name=checkpoint_name,
                validation_definitions=[validation_definition],
                result_format={
                    "result_format": "SUMMARY",
                    "partial_unexpected_count": PARTIAL_UNEXPECTED_COUNT,
                },
            )
        )
        outcome.checkpoint_name = checkpoint_name

        checkpoint_result = checkpoint.run(
            run_id=RunIdentifier(
                run_name=run_name or f"{contract.dataset}-{partition_date}"
            )
        )
        outcome.success = bool(checkpoint_result.success)

        for validation_result in checkpoint_result.run_results.values():
            payload = validation_result.to_json_dict()
            for result in payload.get("results") or []:
                outcome.outcomes.append(_outcome_from_result(result))

        # Row count: every column expectation reports element_count for the
        # same batch, so the maximum is the batch size. Table-level
        # expectations report none, which is why this takes a max rather than
        # the first value it finds.
        counts = [
            o.element_count for o in outcome.outcomes if o.element_count is not None
        ]
        outcome.rows_evaluated = max(counts) if counts else None

    except Exception as exc:
        # A validation that could not run is NOT a pass. It is recorded as a
        # failed validation run carrying the error, so the gate and the score
        # see an absence of evidence rather than evidence of health.
        outcome.success = False
        outcome.error_message = f"{type(exc).__name__}: {exc}"[:2000]
        LOGGER.exception("validation of %s failed to run", contract.dataset)

    outcome.duration_ms = int((time.perf_counter() - started) * 1000)
    LOGGER.info(
        "validated %s: success=%s passed=%d failed=%d max_severity=%s in %dms",
        contract.dataset,
        outcome.success,
        outcome.passed_count,
        len(outcome.failed),
        outcome.max_severity.value,
        outcome.duration_ms,
    )
    return outcome
