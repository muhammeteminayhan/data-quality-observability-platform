"""Read API over observability_db.

    uvicorn src.api.main:app --host 0.0.0.0 --port 8000

Read-only by design, with exactly one exception: acknowledging an incident,
which is an operator action with no equivalent anywhere else. Nothing here can
start a pipeline, change a contract or resolve an incident - resolution
requires a verified healthy measurement from a detector, so an HTTP call must
not be able to shortcut it.

Why this exists alongside Grafana
---------------------------------
Grafana reads the same database for dashboards. This API answers the questions
a dashboard is bad at: "what exactly failed in run X", "show me every version
of this contract", "which incidents are open right now, as JSON I can pipe
somewhere". It is also what the end-to-end tests assert against, because a
JSON response is checkable in a way a rendered panel is not.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from fastapi import FastAPI, HTTPException, Query, status
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from starlette.responses import Response

from src.common.config import get_settings
from src.common.db import fetch_all, fetch_one, observability_connection, ping
from src.common.logging import get_logger
from src.common.storage import ping as storage_ping
from src.observability import incidents as incident_store

LOGGER = get_logger(__name__)

app = FastAPI(
    title="Data Quality & Observability API",
    version="1.0.0",
    description=(
        "Read access to contracts, validation results, detector measurements, "
        "quality scores, gate decisions and incidents."
    ),
)


@app.get("/health", tags=["platform"])
def health() -> dict[str, Any]:
    """Liveness plus real dependency checks.

    Each dependency is probed rather than assumed. A health endpoint that
    returns ok without touching anything is worse than none at all.
    """
    settings = get_settings()
    observability_ok = ping(settings.observability)
    source_ok = ping(settings.source)
    store_ok = storage_ping()
    healthy = observability_ok and source_ok
    return {
        "status": "ok" if healthy else "degraded",
        "checks": {
            "observability_db": observability_ok,
            "source_db": source_ok,
            "object_store": store_ok,
        },
        "checked_at": datetime.now(UTC).isoformat(),
    }


@app.get("/metrics", tags=["platform"])
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/summary", tags=["platform"])
def summary() -> dict[str, Any]:
    """Platform overview: the numbers a human asks for first."""
    with observability_connection() as connection:
        datasets = fetch_all(
            connection,
            """
            SELECT DISTINCT ON (d.dataset_name)
                d.dataset_name, d.criticality, d.source_type, d.layer,
                q.overall_score, q.health_status, q.has_critical_failure,
                q.dimensions_measured, q.unmeasured_dimensions, q.computed_at
            FROM datasets d
            LEFT JOIN quality_scores q USING (dataset_id)
            WHERE d.is_active
            ORDER BY d.dataset_name, q.computed_at DESC NULLS LAST
            """,
        )
        incident_counts = incident_store.summary(connection)
        latest_run = fetch_one(
            connection,
            """
            SELECT run_id, trigger, status, fault_profile, logical_date,
                   started_at, finished_at
            FROM pipeline_runs
            ORDER BY started_at DESC
            LIMIT 1
            """,
        )
        latest_gate = fetch_one(
            connection,
            """
            SELECT decision, max_severity, critical_count, error_count,
                   warning_count, reason, decided_at
            FROM gate_decisions
            ORDER BY decided_at DESC
            LIMIT 1
            """,
        )

    unhealthy = [
        row["dataset_name"]
        for row in datasets
        if row["health_status"] in ("unhealthy", "degraded")
    ]
    never_measured = [row["dataset_name"] for row in datasets if row["health_status"] is None]

    return {
        "datasets_monitored": len(datasets),
        "datasets_unhealthy_or_degraded": unhealthy,
        # Listed separately: a dataset nobody has measured is not healthy, and
        # folding it in with the healthy ones is how a gap becomes invisible.
        "datasets_never_measured": never_measured,
        "incidents": incident_counts,
        "latest_run": latest_run,
        "latest_gate_decision": latest_gate,
        "datasets": datasets,
    }


def _dataset_or_404(connection, dataset_name: str) -> dict[str, Any]:
    row = fetch_one(
        connection,
        "SELECT * FROM datasets WHERE dataset_name = %s",
        (dataset_name,),
    )
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"dataset {dataset_name!r} is not registered. Datasets come from "
                "the contract files, so add contracts/<name>.yml and re-run the "
                "registration step."
            ),
        )
    return row


@app.get("/datasets", tags=["datasets"])
def list_datasets(include_inactive: bool = False) -> dict[str, Any]:
    with observability_connection() as connection:
        rows = fetch_all(
            connection,
            """
            SELECT dataset_id, dataset_name, source_type, layer,
                   physical_location, owner, criticality, description,
                   is_active, created_at, updated_at
            FROM datasets
            WHERE (%s OR is_active)
            ORDER BY dataset_name
            """,
            (include_inactive,),
        )
    return {"count": len(rows), "datasets": rows}


@app.get("/datasets/{dataset_name}", tags=["datasets"])
def get_dataset(dataset_name: str) -> dict[str, Any]:
    with observability_connection() as connection:
        dataset = _dataset_or_404(connection, dataset_name)
        dataset_id = dataset["dataset_id"]
        score = fetch_one(
            connection,
            """
            SELECT * FROM quality_scores
            WHERE dataset_id = %s ORDER BY computed_at DESC LIMIT 1
            """,
            (dataset_id,),
        )
        freshness = fetch_one(
            connection,
            """
            SELECT * FROM freshness_checks
            WHERE dataset_id = %s ORDER BY checked_at DESC LIMIT 1
            """,
            (dataset_id,),
        )
        volume = fetch_one(
            connection,
            """
            SELECT * FROM volume_checks
            WHERE dataset_id = %s ORDER BY checked_at DESC LIMIT 1
            """,
            (dataset_id,),
        )
        schema = fetch_one(
            connection,
            """
            SELECT fingerprint, column_count, columns, captured_at
            FROM schema_snapshots
            WHERE dataset_id = %s ORDER BY captured_at DESC LIMIT 1
            """,
            (dataset_id,),
        )
        open_incidents = fetch_all(
            connection,
            """
            SELECT incident_id, detector, rule_name, severity, status,
                   occurrence_count, first_seen_at, last_seen_at
            FROM incidents
            WHERE dataset_id = %s AND status <> 'RESOLVED'
            ORDER BY last_seen_at DESC
            """,
            (dataset_id,),
        )
    return {
        "dataset": dataset,
        "latest_score": score,
        "latest_freshness": freshness,
        "latest_volume": volume,
        "latest_schema": schema,
        "open_incidents": open_incidents,
    }


@app.get("/datasets/{dataset_name}/scores", tags=["datasets"])
def score_history(dataset_name: str, limit: int = Query(50, ge=1, le=500)) -> dict[str, Any]:
    """Score history from Postgres, not Prometheus.

    This is why business-level history is kept in the database: Prometheus has
    a short retention and cannot join a score to the run and contract version
    that produced it.
    """
    with observability_connection() as connection:
        dataset = _dataset_or_404(connection, dataset_name)
        rows = fetch_all(
            connection,
            """
            SELECT q.run_id, q.overall_score, q.health_status,
                   q.has_critical_failure, q.dimensions_measured,
                   q.unmeasured_dimensions, q.completeness_score,
                   q.uniqueness_score, q.validity_score, q.consistency_score,
                   q.freshness_score, q.volume_score, q.computed_at
            FROM quality_scores q
            WHERE q.dataset_id = %s
            ORDER BY q.computed_at DESC
            LIMIT %s
            """,
            (dataset["dataset_id"], limit),
        )
    return {"dataset": dataset_name, "count": len(rows), "scores": rows}


@app.get("/datasets/{dataset_name}/validations", tags=["datasets"])
def validation_history(
    dataset_name: str,
    only_failures: bool = False,
    limit: int = Query(100, ge=1, le=1000),
) -> dict[str, Any]:
    with observability_connection() as connection:
        dataset = _dataset_or_404(connection, dataset_name)
        rows = fetch_all(
            connection,
            f"""
            SELECT vr.rule_name, vr.expectation_type, vr.column_name,
                   vr.dimension, vr.severity, vr.success, vr.element_count,
                   vr.unexpected_count, vr.unexpected_percent,
                   vr.partial_unexpected, vr.evaluated_at
            FROM validation_results vr
            WHERE vr.dataset_id = %s {'AND vr.success = false' if only_failures else ''}
            ORDER BY vr.evaluated_at DESC
            LIMIT %s
            """,
            (dataset["dataset_id"], limit),
        )
    return {"dataset": dataset_name, "count": len(rows), "results": rows}


@app.get("/contracts/{dataset_name}", tags=["contracts"])
def contract_versions(dataset_name: str) -> dict[str, Any]:
    """Every recorded version of a contract, newest first."""
    with observability_connection() as connection:
        _dataset_or_404(connection, dataset_name)
        rows = fetch_all(
            connection,
            """
            SELECT cv.version, cv.content_sha256, cv.is_active,
                   cv.first_seen_at, cv.last_seen_at, cv.content
            FROM contract_versions cv
            JOIN datasets d USING (dataset_id)
            WHERE d.dataset_name = %s
            ORDER BY cv.first_seen_at DESC
            """,
            (dataset_name,),
        )
    return {"dataset": dataset_name, "count": len(rows), "versions": rows}


@app.get("/incidents", tags=["incidents"])
def list_incidents(
    status_filter: str | None = Query(
        None, alias="status", description="OPEN, ACKNOWLEDGED, SUPPRESSED or RESOLVED"
    ),
    severity: str | None = Query(None, description="INFO, WARNING, ERROR or CRITICAL"),
    dataset: str | None = None,
    limit: int = Query(100, ge=1, le=1000),
) -> dict[str, Any]:
    clauses = ["1 = 1"]
    params: list[Any] = []
    if status_filter:
        clauses.append("i.status = %s")
        params.append(status_filter.upper())
    if severity:
        clauses.append("i.severity = %s")
        params.append(severity.upper())
    if dataset:
        clauses.append("d.dataset_name = %s")
        params.append(dataset)
    params.append(limit)

    with observability_connection() as connection:
        rows = fetch_all(
            connection,
            f"""
            SELECT i.incident_id, d.dataset_name, i.detector, i.rule_name,
                   i.title, i.description, i.severity, i.status,
                   i.occurrence_count, i.first_seen_at, i.last_seen_at,
                   i.suppressed_until, i.acknowledged_at, i.acknowledged_by,
                   i.resolved_at, i.resolution_note,
                   i.observed_value, i.expected_value
            FROM incidents i
            JOIN datasets d USING (dataset_id)
            WHERE {' AND '.join(clauses)}
            ORDER BY
                CASE i.severity
                    WHEN 'CRITICAL' THEN 0 WHEN 'ERROR' THEN 1
                    WHEN 'WARNING' THEN 2 ELSE 3
                END,
                i.last_seen_at DESC
            LIMIT %s
            """,
            tuple(params),
        )
    return {"count": len(rows), "incidents": rows}


@app.get("/incidents/{incident_id}", tags=["incidents"])
def get_incident(incident_id: int) -> dict[str, Any]:
    """One incident with its full event trail.

    The trail is the evidence for the incident's current state - including
    every HEALTHY_CHECK that counted towards a resolution, so a closed
    incident can be audited rather than taken on trust.
    """
    with observability_connection() as connection:
        incident = fetch_one(
            connection,
            """
            SELECT i.*, d.dataset_name
            FROM incidents i JOIN datasets d USING (dataset_id)
            WHERE i.incident_id = %s
            """,
            (incident_id,),
        )
        if incident is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"incident {incident_id} does not exist",
            )
        events = fetch_all(
            connection,
            """
            SELECT event_type, from_status, to_status, severity, actor,
                   detail, occurred_at
            FROM incident_events
            WHERE incident_id = %s
            ORDER BY occurred_at ASC, incident_event_id ASC
            """,
            (incident_id,),
        )
    healthy_checks = sum(1 for e in events if e["event_type"] == "HEALTHY_CHECK")
    return {
        "incident": incident,
        "events": events,
        "verified_healthy_checks": healthy_checks,
        "resolution_requires": incident_store.REQUIRED_HEALTHY_CHECKS,
    }


@app.post("/incidents/{incident_id}/acknowledge", tags=["incidents"])
def acknowledge_incident(
    incident_id: int,
    actor: str = Query(..., min_length=1, max_length=120),
    note: str = Query("", max_length=1000),
) -> dict[str, Any]:
    """Mark an incident as being worked on.

    The one write this API offers. It does NOT resolve the incident:
    resolution needs a verified healthy re-measurement from a detector, and
    letting an HTTP call close an incident would make that guarantee
    decorative.
    """
    with observability_connection() as connection:
        changed = incident_store.acknowledge(
            connection, incident_id, actor=actor, note=note
        )
    if not changed:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                f"incident {incident_id} is not in a live state, so it cannot be "
                "acknowledged"
            ),
        )
    return {
        "incident_id": incident_id,
        "status": "ACKNOWLEDGED",
        "acknowledged_by": actor,
        "note": "acknowledging does not resolve; a verified healthy recheck does",
    }


@app.get("/runs", tags=["runs"])
def list_runs(limit: int = Query(50, ge=1, le=500)) -> dict[str, Any]:
    with observability_connection() as connection:
        rows = fetch_all(
            connection,
            """
            SELECT run_id, trigger, dag_id, dag_run_id, logical_date,
                   fault_profile, status, started_at, finished_at,
                   platform_version, notes
            FROM pipeline_runs
            ORDER BY started_at DESC
            LIMIT %s
            """,
            (limit,),
        )
    return {"count": len(rows), "runs": rows}


@app.get("/runs/{run_id}", tags=["runs"])
def get_run(run_id: str) -> dict[str, Any]:
    """Everything one run measured, in one response."""
    with observability_connection() as connection:
        run = fetch_one(
            connection, "SELECT * FROM pipeline_runs WHERE run_id = %s", (run_id,)
        )
        if run is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"run {run_id} does not exist",
            )
        validations = fetch_all(
            connection,
            """
            SELECT d.dataset_name, vr.suite_name, vr.success,
                   vr.expectations_total, vr.expectations_passed,
                   vr.expectations_failed, vr.rows_evaluated,
                   vr.max_severity, vr.duration_ms, vr.error_message
            FROM validation_runs vr JOIN datasets d USING (dataset_id)
            WHERE vr.run_id = %s ORDER BY d.dataset_name
            """,
            (run_id,),
        )
        scores = fetch_all(
            connection,
            """
            SELECT d.dataset_name, q.overall_score, q.health_status,
                   q.has_critical_failure, q.unmeasured_dimensions
            FROM quality_scores q JOIN datasets d USING (dataset_id)
            WHERE q.run_id = %s ORDER BY d.dataset_name
            """,
            (run_id,),
        )
        gate = fetch_one(
            connection,
            "SELECT * FROM gate_decisions WHERE run_id = %s ORDER BY decided_at DESC LIMIT 1",
            (run_id,),
        )
        dbt = fetch_one(
            connection,
            """
            SELECT invocation_id, command, dbt_version, success,
                   models_total, models_success, models_error,
                   tests_total, tests_pass, tests_fail, tests_warn
            FROM dbt_runs WHERE run_id = %s ORDER BY dbt_run_id DESC LIMIT 1
            """,
            (run_id,),
        )
    return {
        "run": run,
        "validations": validations,
        "scores": scores,
        "gate_decision": gate,
        "dbt": dbt,
    }
