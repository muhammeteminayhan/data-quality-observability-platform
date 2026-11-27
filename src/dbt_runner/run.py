"""Invoke dbt and feed its results into observability_db.

dbt is run as a SUBPROCESS, not through its Python API, for two reasons:

* in the Airflow image dbt lives in an isolated virtualenv at /opt/dbt-venv,
  because dbt requires `pathspec<1.1` while Airflow pins `pathspec==1.1.1`
  and uses it for `.airflowignore`. Importing dbt into the Airflow
  interpreter is precisely what that isolation exists to prevent;
* the exit code and the artifacts are the contract. Driving the library would
  couple this platform to dbt internals that change between minor versions.

What is recorded, and what it means
-----------------------------------
`dbt_runs` keeps model counts and test counts in separate columns because
they answer different questions. A row with models_success=12 and tests_fail=3
describes a pipeline that executed perfectly and produced data that fails its
own assertions - which is exactly the state a "did dbt succeed?" check would
report as green.
"""

from __future__ import annotations

import os
import subprocess
import uuid
from dataclasses import dataclass
from pathlib import Path

from src.common.config import get_settings
from src.common.db import execute, insert_returning_id, observability_connection
from src.common.logging import get_logger
from src.common.severity import Severity
from src.contracts.registry import dataset_ids
from src.dbt_runner.artifacts import DbtRunSummary, dataset_for_node, parse

LOGGER = get_logger(__name__)


@dataclass
class DbtInvocation:
    command: list[str]
    returncode: int
    stdout: str
    stderr: str
    summary: DbtRunSummary | None = None

    @property
    def executed_cleanly(self) -> bool:
        """dbt itself exited 0. NOT a statement about the data."""
        return self.returncode == 0


def invoke(
    *,
    command: str = "build",
    select: str | None = None,
    project_dir: Path | None = None,
    profiles_dir: Path | None = None,
    extra_args: list[str] | None = None,
    timeout_seconds: int = 1800,
) -> DbtInvocation:
    """Run dbt and parse its artifacts."""
    settings = get_settings()
    project = project_dir or settings.dbt_project_dir
    profiles = profiles_dir or settings.dbt_profiles_dir

    argv = [
        settings.dbt_bin,
        command,
        "--project-dir",
        str(project),
        "--profiles-dir",
        str(profiles),
    ]
    if select:
        argv += ["--select", select]
    if extra_args:
        argv += extra_args

    LOGGER.info("running %s", " ".join(argv))
    completed = subprocess.run(  # noqa: S603 - argv is built here, not user input
        argv,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
        cwd=str(project),
        env={**os.environ, "DBT_SEND_ANONYMOUS_USAGE_STATS": "0"},
        check=False,
    )

    invocation = DbtInvocation(
        command=argv,
        returncode=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
    )

    results_path = project / "target" / "run_results.json"
    manifest_path = project / "target" / "manifest.json"
    if results_path.exists():
        invocation.summary = parse(
            results_path,
            manifest_path=manifest_path,
            command=" ".join(argv[1:3]),
        )
    else:
        # No artifact means dbt never got as far as executing anything -
        # a compilation error, a bad profile, an unreachable database. That is
        # a failure to report, not an empty result.
        LOGGER.error(
            "dbt produced no run_results.json (exit %s). stderr: %s",
            completed.returncode,
            completed.stderr[-2000:],
        )

    if completed.returncode != 0:
        LOGGER.warning(
            "dbt exited %s; see the parsed test results for whether the data or "
            "the execution was at fault",
            completed.returncode,
        )
    return invocation


def persist(summary: DbtRunSummary, *, run_id: uuid.UUID) -> int:
    """Store a dbt invocation and every node result."""
    with observability_connection() as connection:
        dbt_run_id = int(
            insert_returning_id(
                connection,
                """
                INSERT INTO dbt_runs (
                    run_id, invocation_id, command, dbt_version,
                    dbt_schema_version, started_at, finished_at,
                    elapsed_seconds, success,
                    models_total, models_success, models_error, models_skipped,
                    tests_total, tests_pass, tests_fail, tests_warn, tests_skipped
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (invocation_id) DO UPDATE SET
                    finished_at = EXCLUDED.finished_at,
                    success = EXCLUDED.success
                RETURNING dbt_run_id
                """,
                (
                    run_id,
                    summary.invocation_id,
                    summary.command,
                    summary.dbt_version,
                    summary.dbt_schema_version,
                    summary.started_at,
                    summary.finished_at,
                    summary.elapsed_seconds,
                    summary.success,
                    len(summary.models),
                    summary.models_success,
                    summary.models_error,
                    summary.models_skipped,
                    len(summary.tests),
                    summary.tests_pass,
                    summary.tests_fail,
                    summary.tests_warn,
                    summary.tests_skipped,
                ),
            )
        )

        known = dataset_ids(connection)
        for node in summary.nodes:
            # Attribute the node to a monitored dataset when it maps onto one.
            # dbt's model names (stg_orders) differ from the contract dataset
            # names (raw.orders), so only an exact match is accepted - a fuzzy
            # guess would attach a finding to the wrong table.
            candidate = dataset_for_node(node)
            dataset_id = known.get(candidate) if candidate else None

            execute(
                connection,
                """
                INSERT INTO dbt_test_results (
                    dbt_run_id, run_id, dataset_id, unique_id, node_name,
                    resource_type, test_type, status, failures, severity,
                    execution_time, message
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    dbt_run_id,
                    run_id,
                    dataset_id,
                    node.unique_id,
                    node.node_name,
                    node.resource_type,
                    node.test_type,
                    node.status,
                    node.failures,
                    node.severity.value if node.severity else None,
                    node.execution_time,
                    (node.message or "")[:2000] or None,
                ),
            )

    LOGGER.info(
        "persisted dbt_run %d (%d node results)", dbt_run_id, len(summary.nodes)
    )
    return dbt_run_id


def run_and_persist(
    *,
    run_id: uuid.UUID,
    command: str = "build",
    select: str | None = None,
) -> tuple[DbtInvocation, Severity]:
    """Run dbt, store the results, and return the DATA severity.

    The returned severity describes the data, not the execution: it comes from
    the test results. The caller still has to look at
    `invocation.executed_cleanly` to know whether dbt itself worked - the two
    failure modes need different responses, and collapsing them is how a
    broken model gets reported as a data-quality incident or vice versa.
    """
    invocation = invoke(command=command, select=select)

    if invocation.summary is None:
        raise RuntimeError(
            "dbt produced no artifacts, so nothing can be said about the data. "
            f"exit={invocation.returncode}; stderr tail: "
            f"{invocation.stderr[-500:]}"
        )

    persist(invocation.summary, run_id=run_id)
    return invocation, invocation.summary.max_severity
