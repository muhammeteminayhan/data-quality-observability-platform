"""Explicit OpenLineage events for tasks the provider cannot extract.

What the Airflow provider does on its own
-----------------------------------------
`apache-airflow-providers-openlineage` emits a START and a COMPLETE/FAIL event
for every task run, with the DAG, task, timings and outcome. For operators it
understands (SQL operators in particular) it also extracts the input and
output datasets automatically.

Where it stops, and why this module exists
------------------------------------------
This platform's work happens in `@task` Python functions that read Postgres
through psycopg and MinIO through boto3. The provider has no extractor for
those: it cannot see inside a Python callable to learn that the function read
`raw.orders` and wrote `observability_db.validation_results`. Without help it
would record that a task ran, with no inputs and no outputs - a lineage graph
of disconnected jobs.

So the custom tasks emit their datasets explicitly, through the same
OpenLineage client and to the same Marquez backend. The events are REAL: they
are produced by the task that actually did the work, at the moment it did it,
carrying the datasets it actually touched. Nothing here draws a diagram from a
static description of the pipeline.

Documented limitations
----------------------
* Column-level lineage is not emitted. The transformations are expressed in
  SQL inside dbt and in pandas inside the engine, and deriving per-column
  provenance from them reliably is beyond what this platform attempts. Facets
  carry table-level inputs and outputs only.
* Emission never fails a task. Lineage is metadata about the run; losing it
  must not lose the run. Failures are logged and counted.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any

from src.common.config import get_settings
from src.common.logging import get_logger

LOGGER = get_logger(__name__)

PRODUCER = "https://github.com/muhammeteminayhan/data-quality-observability-platform"


def _client():
    """An OpenLineage HTTP client pointed at Marquez."""
    from openlineage.client import OpenLineageClient
    from openlineage.client.transport.http import HttpConfig, HttpTransport

    settings = get_settings().lineage
    return OpenLineageClient(
        transport=HttpTransport(
            HttpConfig(url=settings.url, endpoint="api/v1/lineage", timeout=settings.timeout_seconds)
        )
    )


def dataset(namespace: str, name: str, *, fields: list[tuple[str, str]] | None = None):
    """Build an OpenLineage Dataset, with a schema facet when columns are known."""
    from openlineage.client.event_v2 import Dataset
    from openlineage.client.facet_v2 import schema_dataset

    facets: dict[str, Any] = {}
    if fields:
        facets["schema"] = schema_dataset.SchemaDatasetFacet(
            fields=[
                schema_dataset.SchemaDatasetFacetFields(name=name_, type=type_)
                for name_, type_ in fields
            ]
        )
    return Dataset(namespace=namespace, name=name, facets=facets)


def postgres_namespace(host: str, port: int, database: str) -> str:
    """OpenLineage's naming convention for a PostgreSQL dataset."""
    return f"postgres://{host}:{port}/{database}"


def s3_namespace(bucket: str) -> str:
    return f"s3://{bucket}"


def source_dataset(table: str):
    """A table in commerce_db, named the way OpenLineage expects."""
    settings = get_settings().source
    return dataset(
        postgres_namespace(settings.host, settings.port, settings.database), table
    )


def observability_dataset(table: str):
    settings = get_settings().observability
    return dataset(
        postgres_namespace(settings.host, settings.port, settings.database), table
    )


def lake_dataset(prefix: str, partition: str | None = None):
    store = get_settings().object_store
    name = f"{prefix}/dt={partition}" if partition else prefix
    return dataset(s3_namespace(store.bucket), name)


def emit(
    *,
    job_name: str,
    event_type: str,
    run_id: uuid.UUID | str,
    inputs: list[Any] | None = None,
    outputs: list[Any] | None = None,
    namespace: str | None = None,
    description: str | None = None,
) -> bool:
    """Send one RunEvent. Returns True when Marquez accepted it.

    Never raises. A lineage backend being down is not a reason to fail a
    data-quality check, so the failure is logged and reported through the
    return value.
    """
    settings = get_settings().lineage
    if not settings.enabled:
        LOGGER.debug("lineage disabled; not emitting %s for %s", event_type, job_name)
        return False

    try:
        from openlineage.client.event_v2 import Job, Run, RunEvent, RunState
        from openlineage.client.facet_v2 import documentation_job, nominal_time_run

        state = {
            "START": RunState.START,
            "COMPLETE": RunState.COMPLETE,
            "FAIL": RunState.FAIL,
            "ABORT": RunState.ABORT,
        }[event_type]

        job_facets: dict[str, Any] = {}
        if description:
            job_facets["documentation"] = documentation_job.DocumentationJobFacet(
                description=description
            )

        now = datetime.now(UTC)
        event = RunEvent(
            eventType=state,
            eventTime=now.isoformat(),
            run=Run(
                # The platform's own run_id is reused as the OpenLineage run
                # id, so a lineage run in Marquez and a row in pipeline_runs
                # are the same identifier - the graph and the measurements can
                # be joined without a translation table.
                runId=str(run_id),
                facets={
                    "nominalTime": nominal_time_run.NominalTimeRunFacet(
                        nominalStartTime=now.isoformat()
                    )
                },
            ),
            job=Job(
                namespace=namespace or settings.namespace,
                name=job_name,
                facets=job_facets,
            ),
            producer=PRODUCER,
            # schemaURL is NOT passed: in openlineage-python 1.52.0 it is an
            # attrs field with init=False that the client fills in itself, so
            # supplying it raises
            #   RunEvent.__init__() got an unexpected keyword argument 'schemaURL'
            # and every explicit event was silently dropped (the emitter logs
            # the warning and lets the task continue, which is how this was
            # found: Marquez had all 18 provider jobs but zero datasets).
            inputs=inputs or [],
            outputs=outputs or [],
        )
        _client().emit(event)
        LOGGER.info(
            "lineage %s %s (%d in, %d out)",
            event_type,
            job_name,
            len(inputs or []),
            len(outputs or []),
        )
        return True
    except Exception as exc:
        LOGGER.warning(
            "could not emit lineage %s for %s: %s. The task continues: lineage "
            "is metadata about the run, not the run itself.",
            event_type,
            job_name,
            exc,
        )
        return False


@contextmanager
def tracked(
    *,
    job_name: str,
    run_id: uuid.UUID | str,
    inputs: list[Any] | None = None,
    outputs: list[Any] | None = None,
    description: str | None = None,
) -> Iterator[None]:
    """Wrap a block of work in START and COMPLETE/FAIL lineage events.

    Used by the custom Python tasks. The COMPLETE is only sent if the body
    finished; a raising body sends FAIL, so the lineage graph shows what
    actually happened rather than what was intended.
    """
    emit(
        job_name=job_name,
        event_type="START",
        run_id=run_id,
        inputs=inputs,
        outputs=outputs,
        description=description,
    )
    try:
        yield
    except Exception:
        emit(
            job_name=job_name,
            event_type="FAIL",
            run_id=run_id,
            inputs=inputs,
            outputs=outputs,
        )
        raise
    else:
        emit(
            job_name=job_name,
            event_type="COMPLETE",
            run_id=run_id,
            inputs=inputs,
            outputs=outputs,
        )


def verify_backend() -> dict[str, Any]:
    """Ask Marquez what it actually holds.

    Used by the end-to-end tests and by scripts/health_check.sh. The point is
    to prove lineage was INGESTED, not merely emitted: a 200 from a POST only
    means the request was accepted.
    """
    import json
    import urllib.request

    settings = get_settings().lineage
    url = f"{settings.url}/api/v1/namespaces/{settings.namespace}/jobs"
    try:
        with urllib.request.urlopen(url, timeout=settings.timeout_seconds) as response:  # noqa: S310
            payload = json.loads(response.read().decode("utf-8"))
        jobs = payload.get("jobs", [])
        return {
            "reachable": True,
            "namespace": settings.namespace,
            "job_count": len(jobs),
            "jobs": sorted(job.get("name", "") for job in jobs),
        }
    except Exception as exc:
        return {"reachable": False, "namespace": settings.namespace, "error": str(exc)}
