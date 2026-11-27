"""Harness for the end-to-end scenarios.

Each scenario follows the same shape:

    reset to clean  ->  inject one fault  ->  run the real observability pass
                    ->  assert the RIGHT detector caught it at the RIGHT
                        severity, and that the gate did the right thing

Nothing is simulated. ``observe()`` calls the same ``src.observability.cli.run``
the Airflow DAG calls, writing to the same tables the dashboards read.

These tests mutate shared state, so they are serial by construction and each
one resets first rather than trusting the previous test to have cleaned up.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

import pytest


@dataclass
class Observation:
    """What one observability pass concluded about one dataset."""

    dataset: str
    score: float | None
    health: str
    freshness_status: str
    volume_status: str
    max_severity: str
    failed_rules: list[str]
    schema_changes: list[dict[str, Any]]
    drift: list[dict[str, Any]]


@pytest.fixture(scope="module")
def today() -> date:
    return datetime.now(UTC).date()


@pytest.fixture(scope="module")
def seed() -> int:
    from src.common.config import get_settings

    return get_settings().seed


def _generate(partition_date: date, seed: int) -> None:
    """Reload the source with deterministic clean data."""
    from src.generator import parquet_sink, postgres_sink
    from src.generator.cli import scaled_volumes
    from src.generator.constants import DEFAULT_VOLUMES
    from src.generator.records import generate

    DEFAULT_ORDERS = DEFAULT_VOLUMES["orders"]

    # The SAME record count normal operation uses. The volume baseline is
    # per dataset and accumulates across runs, so generating a smaller
    # dataset here would read as a genuine volume collapse against the
    # existing history - the suite would be detecting its own fixture.
    volumes = scaled_volumes(DEFAULT_ORDERS, partition_date=partition_date)
    data = generate(
        seed=seed,
        partition_date=partition_date,
        batch_id=f"e2e-{partition_date.isoformat()}",
        volumes=volumes,
    )
    postgres_sink.write(data, truncate=True)
    parquet_sink.write(data)


@pytest.fixture
def clean_source(stack, today, seed):
    """A freshly generated clean source, and the same again on the way out.

    Restoring afterwards matters as much as before: a scenario that left a
    fault behind would make every later test look like it had found something.
    """
    _generate(today, seed)
    yield
    _generate(today, seed)


@pytest.fixture
def restore_raw_schema(stack):
    """Replay the raw DDL after a scenario has altered the table.

    The schema scenarios use ALTER TABLE, and a removed column cannot be put
    back by reloading rows, so the schema itself is rebuilt from
    sql/source/10_raw_schema.sql. This touches only the synthetic `raw`
    schema, which the generator repopulates immediately afterwards.
    """
    yield
    from pathlib import Path

    from src.common.config import get_settings
    from src.common.db import source_connection

    settings = get_settings()
    statements = (
        Path(settings.home) / "sql" / "source" / "10_raw_schema.sql"
    ).read_text(encoding="utf-8")
    with source_connection(read_only=False) as connection:
        connection.execute("DROP SCHEMA IF EXISTS raw CASCADE")
        connection.execute(statements)


def inject(scenarios: list[str], partition_date: date, seed: int = 7) -> list[Any]:
    """Run fault scenarios against the live source."""
    from src.common.db import source_connection
    from src.generator.fault_injector import inject as run_injection

    with source_connection(read_only=False) as connection:
        return run_injection(
            scenarios, connection=connection, partition_date=partition_date, seed=seed
        )


def observe(partition_date: date, *, enforce_gate: bool = False) -> dict[str, Any]:
    """Run the full observability pass, exactly as the DAG does."""
    from src.common.config import get_settings
    from src.observability.cli import run

    return run(
        partition_date,
        contracts_dir=str(get_settings().contracts_dir),
        enforce_gate=enforce_gate,
    )


def observations(summary: dict[str, Any]) -> dict[str, Observation]:
    """Read back what the pass recorded, per dataset."""
    from src.common.db import fetch_all, observability_connection

    run_id = uuid.UUID(summary["run_id"])
    by_dataset: dict[str, Observation] = {}

    with observability_connection() as connection:
        failed = fetch_all(
            connection,
            """
            SELECT d.dataset_name, vr.rule_name, vr.severity, vr.dimension,
                   vr.unexpected_count
            FROM validation_results vr JOIN datasets d USING (dataset_id)
            WHERE vr.run_id = %s AND vr.success = false
            """,
            (run_id,),
        )
        changes = fetch_all(
            connection,
            """
            SELECT d.dataset_name, sc.change_type, sc.column_name,
                   sc.previous_column, sc.severity, sc.confidence, sc.is_confirmed
            FROM schema_changes sc JOIN datasets d USING (dataset_id)
            WHERE sc.run_id = %s
            """,
            (run_id,),
        )
        drifts = fetch_all(
            connection,
            """
            SELECT d.dataset_name, dd.column_name, dd.method, dd.status,
                   dd.statistic, dd.is_significant, dd.severity
            FROM distribution_drift dd JOIN datasets d USING (dataset_id)
            WHERE dd.run_id = %s
            """,
            (run_id,),
        )

    failures: dict[str, list[dict[str, Any]]] = {}
    for row in failed:
        failures.setdefault(row["dataset_name"], []).append(row)

    for item in summary["datasets"]:
        name = item["dataset"]
        by_dataset[name] = Observation(
            dataset=name,
            score=item["score"],
            health=item["health"],
            freshness_status=item["freshness"],
            volume_status=item["volume"],
            max_severity=item["max_severity"],
            failed_rules=[r["rule_name"] for r in failures.get(name, [])],
            schema_changes=[c for c in changes if c["dataset_name"] == name],
            drift=[d for d in drifts if d["dataset_name"] == name],
        )
    return by_dataset


def failed_rule(observation: Observation, rule_name: str) -> bool:
    return rule_name in observation.failed_rules


def open_incidents(dataset: str) -> list[dict[str, Any]]:
    """Live incidents for one dataset, straight from the database."""
    from src.common.db import fetch_all, observability_connection

    with observability_connection() as connection:
        return fetch_all(
            connection,
            """
            SELECT i.incident_id, i.detector, i.rule_name, i.severity, i.status,
                   i.occurrence_count, i.fingerprint
            FROM incidents i JOIN datasets d USING (dataset_id)
            WHERE d.dataset_name = %s AND i.status <> 'RESOLVED'
            ORDER BY i.incident_id
            """,
            (dataset,),
        )


def incident_events(incident_id: int) -> list[dict[str, Any]]:
    from src.common.db import fetch_all, observability_connection

    with observability_connection() as connection:
        return fetch_all(
            connection,
            """
            SELECT event_type, from_status, to_status, detail, occurred_at
            FROM incident_events WHERE incident_id = %s
            ORDER BY occurred_at, incident_event_id
            """,
            (incident_id,),
        )
