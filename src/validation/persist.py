"""Persist validation outcomes into observability_db.

Written from the CheckpointResult rather than from a GX action, so the insert
shares a transaction with nothing else and a persistence failure is a visible
task error.

Every field listed in the specification is recorded: the run (dataset, suite,
checkpoint, engine + version, timings, totals, max severity) and, per rule,
the dimension, severity, outcome, element and unexpected counts, the
unexpected percentage, the observed and expected values, a bounded sample of
offending values, and the kwargs the rule ran with.
"""

from __future__ import annotations

import json
import uuid
from typing import Any

import psycopg

from src.common.db import insert_returning_id, observability_connection
from src.common.logging import get_logger
from src.common.severity import Severity
from src.contracts.registry import active_contract_version_id, dataset_id_for
from src.validation.runner import RuleOutcome, ValidationOutcome

LOGGER = get_logger(__name__)


def _json(value: Any) -> str | None:
    """Serialise a value for a jsonb column.

    GX puts numpy scalars and Timestamps into results, which json cannot
    encode, so `default=str` is deliberate: losing exact typing in a
    diagnostic field is better than failing the whole insert.
    """
    if value is None:
        return None
    return json.dumps(value, default=str)


def _insert_run(
    connection: psycopg.Connection,
    outcome: ValidationOutcome,
    *,
    run_id: uuid.UUID,
    dataset_id: int,
    contract_version_id: int | None,
) -> int:
    return int(
        insert_returning_id(
            connection,
            """
            INSERT INTO validation_runs (
                run_id, dataset_id, contract_version_id, engine, engine_version,
                suite_name, checkpoint_name, batch_identifier,
                finished_at, duration_ms, success,
                expectations_total, expectations_passed, expectations_failed,
                rows_evaluated, max_severity, error_message
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s,
                    now(), %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING validation_run_id
            """,
            (
                run_id,
                dataset_id,
                contract_version_id,
                outcome.engine,
                outcome.engine_version,
                outcome.suite_name,
                outcome.checkpoint_name,
                outcome.batch_identifier,
                outcome.duration_ms,
                outcome.success,
                len(outcome.outcomes),
                outcome.passed_count,
                len(outcome.failed),
                outcome.rows_evaluated,
                outcome.max_severity.value,
                outcome.error_message,
            ),
        )
    )


def _insert_result(
    connection: psycopg.Connection,
    rule: RuleOutcome,
    *,
    validation_run_id: int,
    run_id: uuid.UUID,
    dataset_id: int,
) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO validation_results (
                validation_run_id, run_id, dataset_id, rule_name,
                expectation_type, column_name, dimension, severity, success,
                element_count, unexpected_count, unexpected_percent,
                observed_value, expected_value, partial_unexpected, kwargs
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                validation_run_id,
                run_id,
                dataset_id,
                rule.rule_name,
                rule.expectation_type,
                rule.column,
                rule.dimension,
                rule.severity.value,
                rule.success,
                rule.element_count,
                rule.unexpected_count,
                rule.unexpected_percent,
                _json(rule.observed_value),
                _json(rule.expected_value),
                _json(rule.partial_unexpected or None),
                _json(rule.kwargs or None),
            ),
        )


def persist(outcome: ValidationOutcome, *, run_id: uuid.UUID) -> int:
    """Store one dataset validation. Returns the validation_run_id."""
    with observability_connection() as connection:
        dataset_id = dataset_id_for(connection, outcome.dataset)
        contract_version_id = active_contract_version_id(connection, dataset_id)

        validation_run_id = _insert_run(
            connection,
            outcome,
            run_id=run_id,
            dataset_id=dataset_id,
            contract_version_id=contract_version_id,
        )
        for rule in outcome.outcomes:
            _insert_result(
                connection,
                rule,
                validation_run_id=validation_run_id,
                run_id=run_id,
                dataset_id=dataset_id,
            )

    LOGGER.info(
        "persisted validation_run %d for %s (%d results)",
        validation_run_id,
        outcome.dataset,
        len(outcome.outcomes),
    )
    return validation_run_id


def persist_all(
    outcomes: list[ValidationOutcome], *, run_id: uuid.UUID
) -> dict[str, int]:
    return {outcome.dataset: persist(outcome, run_id=run_id) for outcome in outcomes}


def worst_severity(outcomes: list[ValidationOutcome]) -> Severity:
    worst = Severity.INFO
    for outcome in outcomes:
        if outcome.max_severity > worst:
            worst = outcome.max_severity
    return worst
