"""One function per Airflow task.

The DAG's tasks are STATELESS: each one reads what it needs from
observability_db using the shared `run_id` and writes its own results back.
Nothing of substance travels through XCom.

Why that matters
----------------
* a task can be retried on its own without the upstream tasks re-running;
* a task can be cleared and re-run by hand while debugging;
* the measurements a later task reads are the ones that were actually
  persisted, so what the gate decides on is exactly what a dashboard shows.
  Passing results through XCom would create a second, invisible copy that can
  disagree with the database.

Only small scalars (counts, decisions) go through XCom, for the UI.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Any

from src.common.db import fetch_all, observability_connection
from src.common.logging import get_logger
from src.common.severity import Severity
from src.contracts import engine as contract_engine
from src.contracts.loader import load_contracts
from src.contracts.models import Contract
from src.contracts.registry import dataset_ids, register_all
from src.observability import (
    anomaly,
    freshness,
    persist,
    quality_score,
    schema_tracker,
    volume,
)
from src.validation.persist import persist as persist_validation
from src.validation.runner import validate_contract

LOGGER = get_logger(__name__)


def _contracts(contracts_dir: str | None = None) -> list[Contract]:
    from src.common.config import get_settings

    return load_contracts(contracts_dir or str(get_settings().contracts_dir))


def _ids() -> dict[str, int]:
    with observability_connection() as connection:
        return dataset_ids(connection)


# ---------------------------------------------------------------------------
# 2. register_contracts
# ---------------------------------------------------------------------------
def register_contracts(contracts_dir: str | None = None) -> dict[str, Any]:
    contracts = _contracts(contracts_dir)
    with observability_connection() as connection:
        registered = register_all(connection, contracts)
    LOGGER.info("registered %d contract(s)", len(registered))
    return {
        "datasets": sorted(registered),
        "count": len(registered),
    }


# ---------------------------------------------------------------------------
# 5 / 6. validation, split by source type so the two run in parallel
# ---------------------------------------------------------------------------
def validate_datasets(
    *,
    run_id: uuid.UUID,
    partition_date: date,
    source_type: str,
    contracts_dir: str | None = None,
) -> dict[str, Any]:
    contracts = [c for c in _contracts(contracts_dir) if c.source.type == source_type]
    ids = _ids()
    results: dict[str, Any] = {"source_type": source_type, "datasets": {}}

    for contract in contracts:
        outcome = validate_contract(contract, partition_date=partition_date)
        outcome.outcomes.extend(
            contract_engine.evaluate(contract, partition_date=partition_date)
        )
        if any(not o.success for o in outcome.outcomes):
            outcome.success = False
        persist_validation(outcome, run_id=run_id)
        results["datasets"][contract.dataset] = {
            "rules": len(outcome.outcomes),
            "failed": len(outcome.failed),
            "max_severity": outcome.max_severity.value,
        }
        _ = ids  # dataset ids are resolved inside persist_validation

    results["total_failed"] = sum(
        item["failed"] for item in results["datasets"].values()
    )
    return results


# ---------------------------------------------------------------------------
# 7. check_freshness
# ---------------------------------------------------------------------------
def check_freshness(
    *,
    run_id: uuid.UUID,
    partition_date: date,
    reference_time: datetime | None = None,
    contracts_dir: str | None = None,
) -> dict[str, Any]:
    ids = _ids()
    summary: dict[str, Any] = {}
    with observability_connection() as connection:
        for contract in _contracts(contracts_dir):
            result = freshness.check(
                contract,
                partition_date=partition_date,
                reference_time=reference_time,
            )
            persist.save_freshness(
                connection, result, run_id=run_id, dataset_id=ids[contract.dataset]
            )
            summary[contract.dataset] = {
                "status": result.status,
                "lag_seconds": result.lag_seconds,
                "source": result.timestamp_source,
                "severity": result.severity.value,
            }
    return summary


# ---------------------------------------------------------------------------
# 8. check_volume
# ---------------------------------------------------------------------------
def check_volume(
    *,
    run_id: uuid.UUID,
    partition_date: date,
    contracts_dir: str | None = None,
) -> dict[str, Any]:
    ids = _ids()
    summary: dict[str, Any] = {}
    with observability_connection() as connection:
        for contract in _contracts(contracts_dir):
            result = volume.check(
                contract,
                partition_date=partition_date,
                dataset_id=ids[contract.dataset],
                exclude_run_id=run_id,
            )
            persist.save_volume(
                connection, result, run_id=run_id, dataset_id=ids[contract.dataset]
            )
            summary[contract.dataset] = {
                "status": result.status,
                "row_count": result.row_count,
                "method": result.baseline_method,
                "history_points": result.history_points,
                "severity": result.severity.value,
            }
    return summary


# ---------------------------------------------------------------------------
# 9. track_schema_drift
# ---------------------------------------------------------------------------
def track_schema_drift(
    *,
    run_id: uuid.UUID,
    partition_date: date,
    contracts_dir: str | None = None,
) -> dict[str, Any]:
    ids = _ids()
    summary: dict[str, Any] = {}
    with observability_connection() as connection:
        for contract in _contracts(contracts_dir):
            dataset_id = ids[contract.dataset]
            previous_row = persist.latest_schema_snapshot(
                connection, dataset_id, before_run_id=run_id
            )
            current = schema_tracker.snapshot(contract, partition_date)
            current_id = persist.save_schema_snapshot(
                connection, current, run_id=run_id, dataset_id=dataset_id
            )

            previous = None
            if previous_row:
                previous = schema_tracker.ObservedSchema(
                    dataset=contract.dataset,
                    columns=tuple(
                        schema_tracker.ObservedColumn(
                            name=column["name"],
                            type=column["type"],
                            nullable=bool(column["nullable"]),
                            position=int(column["position"]),
                        )
                        for column in previous_row["columns"]
                    ),
                )

            changes = schema_tracker.classify(previous, current, contract)
            for change in changes:
                persist.save_schema_change(
                    connection,
                    change,
                    run_id=run_id,
                    dataset_id=dataset_id,
                    from_snapshot_id=int(previous_row["schema_snapshot_id"])
                    if previous_row
                    else None,
                    to_snapshot_id=current_id,
                )
            summary[contract.dataset] = {
                "fingerprint": current.fingerprint[:16],
                "columns": len(current.columns),
                "changes": [
                    {"type": c.change_type, "column": c.column_name, "severity": c.severity.value}
                    for c in changes
                ],
            }
    return summary


# ---------------------------------------------------------------------------
# 10. check_distribution_drift
# ---------------------------------------------------------------------------
def check_distribution_drift(
    *,
    run_id: uuid.UUID,
    partition_date: date,
    contracts_dir: str | None = None,
) -> dict[str, Any]:
    from src.observability.cli import _run_drift

    ids = _ids()
    summary: dict[str, Any] = {}
    with observability_connection() as connection:
        for contract in _contracts(contracts_dir):
            results = _run_drift(
                connection,
                contract,
                partition_date=partition_date,
                run_id=run_id,
                dataset_id=ids[contract.dataset],
            )
            summary[contract.dataset] = [
                {
                    "column": r.column,
                    "method": r.method,
                    "status": r.status,
                    "statistic": float(r.statistic) if r.statistic is not None else None,
                    "significant": r.is_significant,
                }
                for r in results
            ]
    return summary


# ---------------------------------------------------------------------------
# Reading back what earlier tasks measured
# ---------------------------------------------------------------------------
def _run_outcomes(connection, run_id: uuid.UUID) -> dict[str, list[Any]]:
    """Reconstruct the rule outcomes of this run from the database.

    The score and the incident reconciliation both need the per-rule results,
    and they run in later tasks. Reading them back guarantees they see exactly
    what was persisted - the same rows a dashboard will show - rather than a
    copy passed through XCom that could differ.
    """
    from src.validation.runner import RuleOutcome

    rows = fetch_all(
        connection,
        """
        SELECT d.dataset_name, vr.rule_name, vr.expectation_type, vr.column_name,
               vr.dimension, vr.severity, vr.success, vr.element_count,
               vr.unexpected_count, vr.unexpected_percent, vr.observed_value,
               vr.expected_value
        FROM validation_results vr
        JOIN datasets d USING (dataset_id)
        WHERE vr.run_id = %s
        """,
        (run_id,),
    )
    grouped: dict[str, list[Any]] = {}
    for row in rows:
        grouped.setdefault(row["dataset_name"], []).append(
            RuleOutcome(
                rule_name=row["rule_name"],
                dimension=row["dimension"],
                severity=Severity.parse(row["severity"]),
                expectation_type=row["expectation_type"],
                column=row["column_name"],
                success=bool(row["success"]),
                element_count=row["element_count"],
                unexpected_count=row["unexpected_count"],
                unexpected_percent=float(row["unexpected_percent"])
                if row["unexpected_percent"] is not None
                else None,
                observed_value=row["observed_value"],
                expected_value=row["expected_value"],
            )
        )
    return grouped


def _latest_freshness(connection, run_id: uuid.UUID) -> dict[str, Any]:
    rows = fetch_all(
        connection,
        """
        SELECT d.dataset_name, f.*
        FROM freshness_checks f JOIN datasets d USING (dataset_id)
        WHERE f.run_id = %s
        """,
        (run_id,),
    )
    return {row["dataset_name"]: row for row in rows}


def _latest_volume(connection, run_id: uuid.UUID) -> dict[str, Any]:
    rows = fetch_all(
        connection,
        """
        SELECT d.dataset_name, v.*
        FROM volume_checks v JOIN datasets d USING (dataset_id)
        WHERE v.run_id = %s
        """,
        (run_id,),
    )
    return {row["dataset_name"]: row for row in rows}


def _latest_schema_changes(connection, run_id: uuid.UUID) -> dict[str, list[Any]]:
    rows = fetch_all(
        connection,
        """
        SELECT d.dataset_name, sc.change_type, sc.column_name, sc.previous_column,
               sc.previous_type, sc.new_type, sc.confidence, sc.is_confirmed,
               sc.severity, sc.detail
        FROM schema_changes sc JOIN datasets d USING (dataset_id)
        WHERE sc.run_id = %s
        """,
        (run_id,),
    )
    grouped: dict[str, list[Any]] = {}
    for row in rows:
        grouped.setdefault(row["dataset_name"], []).append(
            schema_tracker.SchemaChange(
                change_type=row["change_type"],
                severity=Severity.parse(row["severity"]),
                column_name=row["column_name"],
                previous_column=row["previous_column"],
                previous_type=row["previous_type"],
                new_type=row["new_type"],
                confidence=float(row["confidence"]) if row["confidence"] is not None else None,
                is_confirmed=bool(row["is_confirmed"]),
                detail=row["detail"] or "",
            )
        )
    return grouped


def _rebuild_freshness(row: dict[str, Any] | None):
    if row is None:
        return None
    return freshness.FreshnessResult(
        dataset=row["dataset_name"],
        timestamp_column=row["timestamp_column"],
        timestamp_source=row["timestamp_source"],
        max_timestamp=row["max_timestamp"],
        reference_time=row["reference_time"],
        lag_seconds=row["lag_seconds"],
        warning_after_seconds=row["warning_after_seconds"],
        critical_after_seconds=row["critical_after_seconds"],
        status=row["status"],
        severity=Severity.parse(row["severity"]),
        detail=row["detail"] or "",
    )


def _number(value):
    return float(value) if value is not None else None


def _rebuild_volume(row: dict[str, Any] | None):
    if row is None:
        return None
    return volume.VolumeResult(
        dataset=row["dataset_name"],
        partition_key=row["partition_key"],
        row_count=int(row["row_count"]),
        baseline_method=row["baseline_method"],
        history_points=int(row["history_points"]),
        min_history_required=int(row["min_history_required"]),
        baseline_median=_number(row["baseline_median"]),
        baseline_mad=_number(row["baseline_mad"]),
        robust_z_score=_number(row["robust_z_score"]),
        deviation_percent=_number(row["deviation_percent"]),
        min_expected_rows=row["min_expected_rows"],
        max_expected_rows=row["max_expected_rows"],
        weekday=int(row["weekday"]) if row["weekday"] is not None else 0,
        is_weekend=bool(row["is_weekend"]),
        status=row["status"],
        severity=Severity.parse(row["severity"]),
        detail=row["detail"] or "",
    )


def compute_quality_scores(
    *, run_id: uuid.UUID, contracts_dir: str | None = None
) -> dict[str, Any]:
    """Task 11: the weighted score, from everything measured so far."""
    ids = _ids()
    summary: dict[str, Any] = {}
    with observability_connection() as connection:
        outcomes = _run_outcomes(connection, run_id)
        fresh = _latest_freshness(connection, run_id)
        vols = _latest_volume(connection, run_id)
        schemas = _latest_schema_changes(connection, run_id)

        for contract in _contracts(contracts_dir):
            score = quality_score.compute(
                contract,
                rule_outcomes=outcomes.get(contract.dataset, []),
                freshness_result=_rebuild_freshness(fresh.get(contract.dataset)),
                volume_result=_rebuild_volume(vols.get(contract.dataset)),
                schema_changes=schemas.get(contract.dataset, []),
            )
            persist.save_quality_score(
                connection, score, run_id=run_id, dataset_id=ids[contract.dataset]
            )
            persist.save_metric(
                connection,
                run_id=run_id,
                dataset_id=ids[contract.dataset],
                metric_name="quality_score",
                metric_value=score.overall_score,
                unit="score",
                labels={"health": score.health_status},
            )
            summary[contract.dataset] = {
                "score": score.overall_score,
                "health": score.health_status,
                "critical": score.has_critical_failure,
                "unmeasured": score.unmeasured_dimensions,
            }
    return summary


def build_gate_signals(run_id: uuid.UUID, contracts_dir: str | None = None) -> list[Any]:
    """Collect every severity this run recorded, per dataset."""
    from src.gates.gate import signal_from_parts

    with observability_connection() as connection:
        outcomes = _run_outcomes(connection, run_id)
        fresh = _latest_freshness(connection, run_id)
        vols = _latest_volume(connection, run_id)
        schemas = _latest_schema_changes(connection, run_id)
        drift = fetch_all(
            connection,
            """
            SELECT d.dataset_name, dd.severity
            FROM distribution_drift dd JOIN datasets d USING (dataset_id)
            WHERE dd.run_id = %s AND dd.is_significant
            """,
            (run_id,),
        )
        scores = fetch_all(
            connection,
            """
            SELECT d.dataset_name, q.has_critical_failure, q.health_status,
                   q.overall_score
            FROM quality_scores q JOIN datasets d USING (dataset_id)
            WHERE q.run_id = %s
            """,
            (run_id,),
        )

    drift_by_dataset: dict[str, list[Severity]] = {}
    for row in drift:
        drift_by_dataset.setdefault(row["dataset_name"], []).append(
            Severity.parse(row["severity"])
        )
    score_by_dataset = {row["dataset_name"]: row for row in scores}

    signals = []
    for contract in _contracts(contracts_dir):
        name = contract.dataset
        severities: list[Severity] = []
        failed_rules: list[str] = []

        for outcome in outcomes.get(name, []):
            if not outcome.success:
                severities.append(outcome.severity)
                failed_rules.append(outcome.rule_name)

        fresh_row = fresh.get(name)
        if fresh_row and fresh_row["status"] not in ("ok", "unknown"):
            severities.append(Severity.parse(fresh_row["severity"]))
            failed_rules.append("freshness")

        volume_row = vols.get(name)
        if volume_row and volume_row["status"] not in ("ok", "unknown"):
            severities.append(Severity.parse(volume_row["severity"]))
            failed_rules.append("volume")

        for change in schemas.get(name, []):
            if change.severity > Severity.INFO:
                severities.append(change.severity)
                failed_rules.append("schema:" + change.change_type)

        severities.extend(drift_by_dataset.get(name, []))

        score_row = score_by_dataset.get(name)
        detail = ""
        if score_row:
            detail = (
                "score=" + str(score_row["overall_score"])
                + " health=" + str(score_row["health_status"])
            )
            # The score's own critical flag is included so a CRITICAL finding
            # from any detector reaches the gate even if that detector's
            # severity were somehow lost on the way.
            if score_row["has_critical_failure"]:
                severities.append(Severity.CRITICAL)

        signals.append(
            signal_from_parts(name, severities, failed_rules=failed_rules, detail=detail)
        )
    return signals


def run_quality_gate(
    *,
    run_id: uuid.UUID,
    blocking_datasets: tuple[str, ...] | None = None,
    downstream_tasks: list[str] | None = None,
    contracts_dir: str | None = None,
) -> dict[str, Any]:
    """Task 12: evaluate the gate and RAISE on FAIL.

    Raising is the whole point: it turns a CRITICAL data-quality finding into
    a failed Airflow task, so the DAG run goes red. A gate that only logged
    would let a broken pipeline report success, which the specification
    forbids explicitly.
    """
    from src.gates import gate as gate_module

    signals = build_gate_signals(run_id, contracts_dir)
    decision = gate_module.enforce(
        signals,
        run_id=run_id,
        blocking_datasets=blocking_datasets,
        downstream_tasks=downstream_tasks,
    )
    return {
        "decision": decision.decision,
        "max_severity": decision.max_severity.value,
        "critical_count": decision.critical_count,
        "error_count": decision.error_count,
        "warning_count": decision.warning_count,
        "reason": decision.reason,
    }


def reconcile_incidents(
    *, run_id: uuid.UUID, contracts_dir: str | None = None
) -> dict[str, Any]:
    """Task 15: open, deduplicate and resolve incidents."""
    ids = _ids()
    summary: dict[str, Any] = {"opened": 0, "recurred": 0, "suppressed": 0, "resolved": 0}
    alertable: list[dict[str, Any]] = []

    with observability_connection() as connection:
        outcomes = _run_outcomes(connection, run_id)
        fresh = _latest_freshness(connection, run_id)
        vols = _latest_volume(connection, run_id)
        schemas = _latest_schema_changes(connection, run_id)
        scores = {
            row["dataset_name"]: row
            for row in fetch_all(
                connection,
                """
                SELECT d.dataset_name, q.*
                FROM quality_scores q JOIN datasets d USING (dataset_id)
                WHERE q.run_id = %s
                """,
                (run_id,),
            )
        }

        for contract in _contracts(contracts_dir):
            name = contract.dataset
            score_row = scores.get(name)
            score = None
            if score_row:
                score = quality_score.QualityScore(
                    dataset=name,
                    overall_score=float(score_row["overall_score"]),
                    dimensions_measured=int(score_row["dimensions_measured"]),
                    dimensions_total=int(score_row["dimensions_total"]),
                    unmeasured_dimensions=list(score_row["unmeasured_dimensions"] or []),
                    has_critical_failure=bool(score_row["has_critical_failure"]),
                    health_status=score_row["health_status"],
                    detail=str(score_row["health_status"]),
                )

            result = anomaly.reconcile(
                connection,
                contract,
                run_id=run_id,
                dataset_id=ids[name],
                rule_outcomes=outcomes.get(name, []),
                freshness_result=_rebuild_freshness(fresh.get(name)),
                volume_result=_rebuild_volume(vols.get(name)),
                schema_changes=schemas.get(name, []),
                drift_results=[],
                score=score,
            )
            for action in result.actions:
                key = action.action.lower()
                if key in summary:
                    summary[key] += 1
                if action.should_alert:
                    alertable.append(
                        {
                            "incident_id": action.incident_id,
                            "dataset": action.dataset,
                            "severity": action.severity.value,
                            "title": action.title,
                            "action": action.action,
                        }
                    )
            summary["resolved"] += len(result.resolved)

    summary["alertable"] = alertable
    return summary


def dispatch_alerts(
    *, run_id: uuid.UUID, alertable: list[dict[str, Any]] | None = None
) -> dict[str, Any]:
    """Task 16: send the alerts this run earned, and record every attempt.

    Reads the live incidents rather than trusting a list handed over from the
    previous task, so a retry of this task alone still finds the right work.
    An incident inside its cooldown window is skipped here - suppression is
    decided by the incident layer, not by the sender.
    """
    import json
    import urllib.error
    import urllib.request

    from src.common.config import get_settings

    settings = get_settings()
    sent = 0
    duplicates = 0
    failed = 0
    results: list[dict[str, Any]] = []

    with observability_connection() as connection:
        rows = fetch_all(
            connection,
            """
            SELECT i.incident_id, d.dataset_name, i.detector, i.rule_name,
                   i.title, i.description, i.severity, i.status,
                   i.occurrence_count
            FROM incidents i JOIN datasets d USING (dataset_id)
            WHERE i.status = 'OPEN'
              AND i.last_run_id = %s
              AND i.severity IN ('WARNING', 'ERROR', 'CRITICAL')
            ORDER BY i.incident_id
            """,
            (run_id,),
        )

    for row in rows:
        payload = {
            "status": "firing",
            "dataset": row["dataset_name"],
            "rule": row["rule_name"],
            "severity": row["severity"],
            "title": row["title"],
            "message": row["description"] or row["title"],
            "incident_id": int(row["incident_id"]),
        }
        request = urllib.request.Request(  # noqa: S310 - fixed in-network URL
            settings.alerting.webhook_url,
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "X-DQ-Token": settings.alerting.webhook_token,
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=10) as response:  # noqa: S310
                body = json.loads(response.read().decode("utf-8"))
            outcome = body.get("result", "received")
            if outcome == "duplicate":
                duplicates += 1
            else:
                sent += 1
            results.append({"incident_id": row["incident_id"], "result": outcome})
        except Exception as exc:
            # A failed delivery is recorded, not swallowed. An alerting path
            # that silently drops messages is worse than none, because the
            # dashboard still says an alert was raised.
            failed += 1
            LOGGER.error(
                "alert delivery failed for incident %s: %s", row["incident_id"], exc
            )
            with observability_connection() as connection:
                from src.observability.persist import _json as to_json

                connection.execute(
                    """
                    INSERT INTO alert_deliveries (
                        incident_id, run_id, channel, request_fingerprint,
                        payload, status, error_message
                    )
                    VALUES (%s, %s, 'webhook', %s, %s, 'failed', %s)
                    ON CONFLICT (channel, request_fingerprint) DO NOTHING
                    """,
                    (
                        row["incident_id"],
                        run_id,
                        f"failed:{row['incident_id']}:{run_id}",
                        to_json(payload),
                        str(exc)[:500],
                    ),
                )
            results.append({"incident_id": row["incident_id"], "result": "failed"})

    LOGGER.info(
        "alerts: %d sent, %d duplicate, %d failed (of %d candidate incidents)",
        sent,
        duplicates,
        failed,
        len(rows),
    )
    return {
        "candidates": len(rows),
        "sent": sent,
        "duplicates": duplicates,
        "failed": failed,
        "results": results,
    }
