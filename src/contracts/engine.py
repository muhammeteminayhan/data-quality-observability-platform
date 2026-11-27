"""Contract rules the SQL engine cannot evaluate.

Two kinds live here:

``pandas_query``
    A Parquet dataset is read with the pandas execution engine, which has no
    SQL. A cross-column identity such as
    ``stock_available == stock_on_hand - stock_reserved`` therefore cannot be
    expressed as a GX column expectation (and GX's own
    ExpectMulticolumnSumToEqual takes a scalar total, not another column).
    The rule states a query whose MATCHING rows are the violations - the same
    polarity as UnexpectedRowsExpectation, so the two rule kinds read alike.

Everything else goes through Great Expectations. This module exists to cover a
genuine gap, not to re-implement a validation engine.
"""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

from src.common.logging import get_logger
from src.common.storage import read_partition
from src.contracts.models import Contract, ContractError, QualityRule
from src.validation.runner import RuleOutcome

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd

LOGGER = get_logger(__name__)

PARTIAL_UNEXPECTED_COUNT = 10


def _evaluate_pandas_query(rule: QualityRule, frame: pd.DataFrame) -> RuleOutcome:
    query = rule.kwargs.get("query")
    if not query:
        raise ContractError(f"{rule.name}: a pandas_query rule needs kwargs.query")

    element_count = int(len(frame))
    try:
        violations = frame.query(query)
    except Exception as exc:
        # A query that cannot run is NOT a pass. It is reported as a failure
        # carrying the error, so a typo in a contract cannot silently retire a
        # rule.
        return RuleOutcome(
            rule_name=rule.name,
            dimension=rule.dimension,
            severity=rule.severity,
            expectation_type="pandas_query",
            column=None,
            success=False,
            element_count=element_count,
            unexpected_count=None,
            kwargs=dict(rule.kwargs),
            exception_message=f"{type(exc).__name__}: {exc}",
        )

    unexpected = int(len(violations))
    percent = (unexpected / element_count * 100.0) if element_count else 0.0
    allowed = rule.max_unexpected_percent
    success = percent <= allowed

    sample = (
        violations.head(PARTIAL_UNEXPECTED_COUNT).to_dict(orient="records")
        if unexpected
        else []
    )

    return RuleOutcome(
        rule_name=rule.name,
        dimension=rule.dimension,
        severity=rule.severity,
        expectation_type="pandas_query",
        column=None,
        success=success,
        element_count=element_count,
        unexpected_count=unexpected,
        unexpected_percent=round(percent, 5),
        observed_value=unexpected,
        expected_value=f"<= {allowed}% of rows",
        partial_unexpected=sample,
        kwargs=dict(rule.kwargs),
    )


def evaluate(
    contract: Contract,
    *,
    partition_date: date,
    frame: pd.DataFrame | None = None,
) -> list[RuleOutcome]:
    """Evaluate the engine-owned rules of one contract."""
    from src.validation.suite_builder import engine_rules

    rules = engine_rules(contract)
    if not rules:
        return []

    if contract.source.type != "parquet":
        raise ContractError(
            f"{contract.dataset}: pandas_query rules are only supported for parquet "
            "datasets; a postgres dataset should use an unexpected_rows rule"
        )

    data = (
        frame
        if frame is not None
        else read_partition(contract.source.location, partition_date.isoformat())
    )

    if data.empty:
        # No data is not a pass. Each rule is reported as failed with an
        # explicit reason, which the score then treats as a measured failure
        # rather than a missing measurement.
        LOGGER.warning(
            "%s partition %s is empty; %d engine rule(s) cannot be evaluated",
            contract.dataset,
            partition_date,
            len(rules),
        )
        return [
            RuleOutcome(
                rule_name=rule.name,
                dimension=rule.dimension,
                severity=rule.severity,
                expectation_type=rule.expectation,
                column=None,
                success=False,
                element_count=0,
                kwargs=dict(rule.kwargs),
                exception_message="partition contains no rows",
            )
            for rule in rules
        ]

    outcomes = [_evaluate_pandas_query(rule, data) for rule in rules]
    LOGGER.info(
        "contract engine evaluated %d rule(s) for %s: %d failed",
        len(outcomes),
        contract.dataset,
        sum(1 for o in outcomes if not o.success),
    )
    return outcomes
