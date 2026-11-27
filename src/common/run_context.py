"""The pipeline run - the correlation parent of every observation.

One row in ``pipeline_runs`` per execution of the platform. Every check,
score, gate decision and incident event carries that ``run_id``, so any
number shown on a dashboard can be traced back to the exact run that
measured it.

Inside Airflow the run_id is derived from the DAG run so that all 17 tasks of
one DAG run share a single parent row, rather than each task opening its own
and fragmenting the correlation.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

from src.common.config import get_settings
from src.common.db import execute, fetch_one, observability_connection
from src.common.logging import get_logger

LOGGER = get_logger(__name__)

# A fixed namespace so the same DAG run always maps to the same UUID. Any
# constant UUID works; this one was generated once and must not change, or
# previously recorded runs would stop matching.
_RUN_NAMESPACE = uuid.UUID("6f1f2d18-6b0e-5a7a-9c3d-2a4b8e7f1c55")


@dataclass
class RunContext:
    """Identity and provenance of one platform execution."""

    run_id: uuid.UUID
    trigger: str
    logical_date: date
    fault_profile: str = "clean"
    dag_id: str | None = None
    dag_run_id: str | None = None
    task_id: str | None = None
    notes: str | None = None

    @property
    def run_id_str(self) -> str:
        return str(self.run_id)


def deterministic_run_id(dag_run_id: str) -> uuid.UUID:
    """Map an Airflow dag_run_id onto a stable UUID.

    uuid5 rather than uuid4: every task in the DAG run computes the same value
    independently, with no need to pass state between tasks or to read XCom
    before the parent row exists.
    """
    return uuid.uuid5(_RUN_NAMESPACE, dag_run_id)


def from_airflow_context(context: dict[str, Any], *, fault_profile: str | None = None) -> RunContext:
    """Build a RunContext from an Airflow task context."""
    dag_run = context.get("dag_run")
    task_instance = context.get("task_instance") or context.get("ti")

    dag_run_id = getattr(dag_run, "run_id", None) or context.get("run_id") or str(uuid.uuid4())
    dag_id = getattr(getattr(dag_run, "dag", None), "dag_id", None) or context.get("dag").dag_id  # type: ignore[union-attr]

    logical = context.get("logical_date") or context.get("data_interval_start")
    if isinstance(logical, datetime):
        logical_date = logical.date()
    elif isinstance(logical, date):
        logical_date = logical
    else:
        logical_date = datetime.now(UTC).date()

    # DAG params win over the environment: a manual run that sets
    # fault_profile="volume_drop" must be recorded as such.
    params = context.get("params") or {}
    resolved_profile = fault_profile or params.get("fault_profile") or "clean"

    return RunContext(
        run_id=deterministic_run_id(str(dag_run_id)),
        trigger="airflow",
        logical_date=logical_date,
        fault_profile=str(resolved_profile),
        dag_id=dag_id,
        dag_run_id=str(dag_run_id),
        task_id=getattr(task_instance, "task_id", None),
    )


def new_cli_run(
    *,
    logical_date: date | None = None,
    fault_profile: str = "clean",
    trigger: str = "cli",
    notes: str | None = None,
) -> RunContext:
    """A run started from the command line or from a test."""
    return RunContext(
        run_id=uuid.uuid4(),
        trigger=trigger,
        logical_date=logical_date or datetime.now(UTC).date(),
        fault_profile=fault_profile,
        notes=notes,
    )


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


def ensure_run(context: RunContext) -> uuid.UUID:
    """Insert the parent row if it is not already there.

    Idempotent because all 17 Airflow tasks call it: the first one creates the
    row and the rest are no-ops. ON CONFLICT DO NOTHING rather than a
    read-then-write, so two tasks starting at the same moment cannot race.
    """
    with observability_connection() as connection:
        execute(
            connection,
            """
            INSERT INTO pipeline_runs (
                run_id, trigger, dag_id, dag_run_id, task_id,
                logical_date, fault_profile, platform_version, notes
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id) DO NOTHING
            """,
            (
                context.run_id,
                context.trigger,
                context.dag_id,
                context.dag_run_id,
                context.task_id,
                context.logical_date,
                context.fault_profile,
                get_settings().platform_version,
                context.notes,
            ),
        )
    return context.run_id


def finish_run(run_id: uuid.UUID, status: str, notes: str | None = None) -> None:
    """Close the parent row.

    ``status`` is one of running / success / failed / blocked. ``blocked``
    records that the quality gate stopped the pipeline, which is a different
    outcome from a crash and is worth telling apart on a dashboard.
    """
    if status not in {"running", "success", "failed", "blocked"}:
        raise ValueError(f"unexpected run status {status!r}")

    with observability_connection() as connection:
        execute(
            connection,
            """
            UPDATE pipeline_runs
            SET status = %s,
                finished_at = now(),
                notes = COALESCE(%s, notes)
            WHERE run_id = %s
            """,
            (status, notes, run_id),
        )
    LOGGER.info("run %s finished with status=%s", run_id, status)


def get_run(run_id: uuid.UUID) -> dict[str, Any] | None:
    with observability_connection() as connection:
        return fetch_one(
            connection, "SELECT * FROM pipeline_runs WHERE run_id = %s", (run_id,)
        )


@contextmanager
def run_scope(context: RunContext) -> Iterator[RunContext]:
    """Open a run, close it as success, or as failed when the body raises.

    Used by the CLI entrypoints. The Airflow tasks do not use this - there the
    run spans many tasks, so opening and closing are separate tasks of the DAG.
    """
    ensure_run(context)
    try:
        yield context
    except Exception as exc:
        finish_run(context.run_id, "failed", notes=f"{type(exc).__name__}: {exc}"[:500])
        raise
    else:
        finish_run(context.run_id, "success")


def current_airflow_dag_run_id() -> str | None:
    """Read the DAG run id from the task environment, when there is one.

    Airflow exports it to the task process, which lets a module called deep
    inside a task correlate itself without the context being threaded through
    every function signature.
    """
    return os.environ.get("AIRFLOW_CTX_DAG_RUN_ID")
