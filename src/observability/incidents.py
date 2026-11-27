"""Incidents: deduplication, cooldown, and resolution that requires evidence.

Fingerprint
-----------
    sha256(dataset | detector | rule_name | dimension)

One recurring problem therefore updates ONE incident instead of creating a new
row every run. `ux_incidents_active_fingerprint` is a partial unique index over
OPEN/ACKNOWLEDGED/SUPPRESSED, so the database refuses a second live incident
for the same fingerprint even if the code tried.

Cooldown
--------
After an alert is emitted the incident is suppressed for
`cooldown_minutes`. During that window recurrences still update
`occurrence_count` and `last_seen_at` - the problem is not hidden - but no new
alert is sent. This is what stops a broken nightly load producing hundreds of
identical notifications.

Resolution: the rule this module exists to enforce
--------------------------------------------------
An incident is NEVER resolved because its alert went quiet. Silence has at
least three causes and only one of them is "fixed":

  * the problem really is fixed;
  * the check did not run at all (upstream task failed, DAG paused);
  * the dataset vanished, so there is nothing left to measure.

Treating those alike is how a monitoring system reports all-clear during an
outage. So resolution requires a POSITIVE, verified healthy re-measurement:
`record_healthy()` is called only when a detector actually ran and actually
passed. It writes a HEALTHY_CHECK event, and only once
`REQUIRED_HEALTHY_CHECKS` of them have accumulated does the incident move to
RESOLVED. A run in which the dataset was not measured writes nothing, so the
incident simply stays open - which is the honest state.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from typing import Any

import psycopg

from src.common.config import get_settings
from src.common.db import execute, fetch_all, fetch_one, insert_returning_id
from src.common.logging import get_logger
from src.common.severity import Severity

LOGGER = get_logger(__name__)

# Consecutive verified healthy measurements required before an incident is
# resolved. More than one means a flapping dataset does not oscillate between
# OPEN and RESOLVED on alternate runs.
REQUIRED_HEALTHY_CHECKS = 2

LIVE_STATUSES = ("OPEN", "ACKNOWLEDGED", "SUPPRESSED")


@dataclass
class Finding:
    """Something a detector observed about a dataset this run."""

    dataset: str
    detector: str
    title: str
    severity: Severity
    description: str = ""
    rule_name: str | None = None
    dimension: str | None = None
    observed_value: Any = None
    expected_value: Any = None

    @property
    def fingerprint(self) -> str:
        parts = [
            self.dataset,
            self.detector,
            self.rule_name or "-",
            self.dimension or "-",
        ]
        return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


@dataclass
class IncidentAction:
    """What reconciliation did, so the caller can alert on it."""

    incident_id: int
    fingerprint: str
    dataset: str
    action: str  # OPENED | RECURRED | SUPPRESSED | ESCALATED | RESOLVED | HEALTHY_CHECK
    severity: Severity
    should_alert: bool
    title: str
    detail: str = ""
    occurrence_count: int = 1


def _json(value: Any) -> str | None:
    return None if value is None else json.dumps(value, default=str)


def _record_event(
    connection: psycopg.Connection,
    incident_id: int,
    *,
    run_id: uuid.UUID | None,
    event_type: str,
    from_status: str | None = None,
    to_status: str | None = None,
    severity: Severity | None = None,
    detail: str = "",
    payload: Any = None,
    actor: str = "platform",
) -> None:
    execute(
        connection,
        """
        INSERT INTO incident_events (
            incident_id, run_id, event_type, from_status, to_status,
            severity, actor, detail, payload
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            incident_id,
            run_id,
            event_type,
            from_status,
            to_status,
            severity.value if severity else None,
            actor,
            detail,
            _json(payload),
        ),
    )


def find_live(
    connection: psycopg.Connection, fingerprint: str
) -> dict[str, Any] | None:
    return fetch_one(
        connection,
        f"""
        SELECT * FROM incidents
        WHERE fingerprint = %s AND status IN {LIVE_STATUSES}
        """,
        (fingerprint,),
    )


def healthy_check_count(connection: psycopg.Connection, incident_id: int) -> int:
    """Consecutive HEALTHY_CHECK events since the last non-healthy event.

    Counted as a streak, not a total: a problem that recurs resets the count,
    so two healthy runs separated by a failure do not add up to a resolution.
    """
    rows = fetch_all(
        connection,
        """
        SELECT event_type
        FROM incident_events
        WHERE incident_id = %s
        ORDER BY occurred_at DESC, incident_event_id DESC
        LIMIT 50
        """,
        (incident_id,),
    )
    streak = 0
    for row in rows:
        if row["event_type"] == "HEALTHY_CHECK":
            streak += 1
        else:
            break
    return streak


def open_or_update(
    connection: psycopg.Connection,
    finding: Finding,
    *,
    run_id: uuid.UUID,
    dataset_id: int,
    cooldown_minutes: int | None = None,
) -> IncidentAction:
    """Open a new incident, or record a recurrence of a live one."""
    cooldown = (
        cooldown_minutes
        if cooldown_minutes is not None
        else get_settings().alerting.cooldown_minutes
    )
    fingerprint = finding.fingerprint
    existing = find_live(connection, fingerprint)

    if existing is None:
        incident_id = int(
            insert_returning_id(
                connection,
                """
                INSERT INTO incidents (
                    fingerprint, dataset_id, detector, rule_name, title,
                    description, severity, status, first_run_id, last_run_id,
                    suppressed_until, observed_value, expected_value
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'OPEN', %s, %s,
                        now() + make_interval(mins => %s), %s, %s)
                RETURNING incident_id
                """,
                (
                    fingerprint,
                    dataset_id,
                    finding.detector,
                    finding.rule_name,
                    finding.title,
                    finding.description,
                    finding.severity.value,
                    run_id,
                    run_id,
                    cooldown,
                    _json(finding.observed_value),
                    _json(finding.expected_value),
                ),
            )
        )
        _record_event(
            connection,
            incident_id,
            run_id=run_id,
            event_type="OPENED",
            to_status="OPEN",
            severity=finding.severity,
            detail=finding.description or finding.title,
            payload={"observed": finding.observed_value, "expected": finding.expected_value},
        )
        LOGGER.info(
            "incident %d OPENED for %s (%s)", incident_id, finding.dataset, finding.detector
        )
        return IncidentAction(
            incident_id=incident_id,
            fingerprint=fingerprint,
            dataset=finding.dataset,
            action="OPENED",
            severity=finding.severity,
            should_alert=finding.severity.alerts,
            title=finding.title,
            detail=finding.description,
            occurrence_count=1,
        )

    incident_id = int(existing["incident_id"])
    previous_severity = Severity.parse(existing["severity"])
    occurrence = int(existing["occurrence_count"]) + 1
    suppressed_until = existing["suppressed_until"]

    # Escalation: a problem that gets worse is worth a fresh alert even inside
    # the cooldown window, because the previous alert understated it.
    escalated = finding.severity > previous_severity
    within_cooldown = suppressed_until is not None and not escalated

    new_status = existing["status"]
    if within_cooldown and new_status == "OPEN":
        new_status = "SUPPRESSED"
    elif escalated and new_status == "SUPPRESSED":
        new_status = "OPEN"

    effective_severity = finding.severity if escalated else previous_severity

    execute(
        connection,
        """
        UPDATE incidents
        SET last_seen_at = now(),
            occurrence_count = %s,
            last_run_id = %s,
            severity = %s,
            status = %s,
            observed_value = %s,
            suppressed_until = CASE
                WHEN %s THEN now() + make_interval(mins => %s)
                ELSE suppressed_until
            END
        WHERE incident_id = %s
        """,
        (
            occurrence,
            run_id,
            effective_severity.value,
            new_status,
            _json(finding.observed_value),
            escalated,
            cooldown,
            incident_id,
        ),
    )

    if escalated:
        action = "ESCALATED"
        should_alert = True
        detail = (
            f"severity rose from {previous_severity.value} to {finding.severity.value}; "
            "alerting again despite the cooldown"
        )
    elif within_cooldown:
        action = "SUPPRESSED"
        should_alert = False
        detail = (
            f"occurrence {occurrence}; suppressed until {suppressed_until} so one "
            "recurring problem does not emit a new alert every run"
        )
    else:
        action = "RECURRED"
        should_alert = finding.severity.alerts
        detail = f"occurrence {occurrence}; cooldown had expired"

    _record_event(
        connection,
        incident_id,
        run_id=run_id,
        event_type=action,
        from_status=existing["status"],
        to_status=new_status,
        severity=effective_severity,
        detail=detail,
    )
    return IncidentAction(
        incident_id=incident_id,
        fingerprint=fingerprint,
        dataset=finding.dataset,
        action=action,
        severity=effective_severity,
        should_alert=should_alert,
        title=finding.title,
        detail=detail,
        occurrence_count=occurrence,
    )


def record_healthy(
    connection: psycopg.Connection,
    *,
    dataset: str,
    detector: str,
    rule_name: str | None,
    dimension: str | None,
    run_id: uuid.UUID,
    detail: str = "",
) -> IncidentAction | None:
    """Record a VERIFIED healthy measurement for a live incident.

    Called only when a detector actually ran and actually passed. It is never
    called because an alert stopped arriving - that distinction is the whole
    point of this function.

    Returns an action when the incident is resolved, None while it is still
    accumulating healthy checks or when there is no live incident.
    """
    finding = Finding(
        dataset=dataset,
        detector=detector,
        title="",
        severity=Severity.INFO,
        rule_name=rule_name,
        dimension=dimension,
    )
    existing = find_live(connection, finding.fingerprint)
    if existing is None:
        return None

    incident_id = int(existing["incident_id"])
    _record_event(
        connection,
        incident_id,
        run_id=run_id,
        event_type="HEALTHY_CHECK",
        from_status=existing["status"],
        to_status=existing["status"],
        detail=detail
        or f"{detector} ran and passed for {dataset}: a positive re-measurement",
    )

    streak = healthy_check_count(connection, incident_id)
    if streak < REQUIRED_HEALTHY_CHECKS:
        LOGGER.info(
            "incident %d has %d/%d verified healthy checks; staying %s",
            incident_id,
            streak,
            REQUIRED_HEALTHY_CHECKS,
            existing["status"],
        )
        return None

    execute(
        connection,
        """
        UPDATE incidents
        SET status = 'RESOLVED',
            resolved_at = now(),
            resolved_run_id = %s,
            suppressed_until = NULL,
            resolution_note = %s
        WHERE incident_id = %s
        """,
        (
            run_id,
            f"resolved after {streak} consecutive verified healthy measurements",
            incident_id,
        ),
    )
    _record_event(
        connection,
        incident_id,
        run_id=run_id,
        event_type="RESOLVED",
        from_status=existing["status"],
        to_status="RESOLVED",
        detail=(
            f"{streak} consecutive verified healthy measurements; resolution is "
            "evidence-based, never inferred from an absence of alerts"
        ),
    )
    LOGGER.info("incident %d RESOLVED after %d healthy checks", incident_id, streak)
    return IncidentAction(
        incident_id=incident_id,
        fingerprint=finding.fingerprint,
        dataset=dataset,
        action="RESOLVED",
        severity=Severity.INFO,
        should_alert=True,
        title=existing["title"],
        detail=f"resolved after {streak} verified healthy measurements",
        occurrence_count=int(existing["occurrence_count"]),
    )


def acknowledge(
    connection: psycopg.Connection, incident_id: int, *, actor: str, note: str = ""
) -> bool:
    """Mark an incident as being worked on. Does not resolve it."""
    row = fetch_one(
        connection,
        "SELECT status FROM incidents WHERE incident_id = %s",
        (incident_id,),
    )
    if row is None or row["status"] not in LIVE_STATUSES:
        return False
    execute(
        connection,
        """
        UPDATE incidents
        SET status = 'ACKNOWLEDGED', acknowledged_at = now(), acknowledged_by = %s
        WHERE incident_id = %s
        """,
        (actor, incident_id),
    )
    _record_event(
        connection,
        incident_id,
        run_id=None,
        event_type="ACKNOWLEDGED",
        from_status=row["status"],
        to_status="ACKNOWLEDGED",
        actor=actor,
        detail=note or "acknowledged by an operator",
    )
    return True


def open_incidents(connection: psycopg.Connection) -> list[dict[str, Any]]:
    return fetch_all(
        connection,
        f"""
        SELECT i.*, d.dataset_name
        FROM incidents i
        JOIN datasets d USING (dataset_id)
        WHERE i.status IN {LIVE_STATUSES}
        ORDER BY
            CASE i.severity
                WHEN 'CRITICAL' THEN 0 WHEN 'ERROR' THEN 1
                WHEN 'WARNING' THEN 2 ELSE 3
            END,
            i.last_seen_at DESC
        """,
    )


def summary(connection: psycopg.Connection) -> dict[str, int]:
    rows = fetch_all(
        connection,
        "SELECT status, count(*) AS n FROM incidents GROUP BY status",
    )
    return {row["status"]: int(row["n"]) for row in rows}


def live_for_dataset(
    connection: psycopg.Connection, dataset_id: int
) -> list[dict[str, Any]]:
    """Every live incident for one dataset.

    Used by reconciliation to decide which incidents this run is in a position
    to resolve: only those whose detector actually ran.
    """
    return fetch_all(
        connection,
        f"""
        SELECT incident_id, fingerprint, detector, rule_name, status, severity, title
        FROM incidents
        WHERE dataset_id = %s AND status IN {LIVE_STATUSES}
        """,
        (dataset_id,),
    )


def record_healthy_by_fingerprint(
    connection: psycopg.Connection,
    *,
    incident_id: int,
    fingerprint: str,
    dataset: str,
    run_id: uuid.UUID,
    detail: str,
) -> IncidentAction | None:
    """Record a verified healthy measurement for a KNOWN live incident.

    The fingerprint-based variant of ``record_healthy``. Some detectors key
    their findings on the specific thing that broke - a schema change on one
    column, drift on one column - so the only way to say "that specific
    problem is gone" is to start from the open incident and confirm its
    finding is absent from a run in which that detector did execute.
    """
    row = fetch_one(
        connection,
        "SELECT status, occurrence_count, title FROM incidents WHERE incident_id = %s",
        (incident_id,),
    )
    if row is None or row["status"] not in LIVE_STATUSES:
        return None

    _record_event(
        connection,
        incident_id,
        run_id=run_id,
        event_type="HEALTHY_CHECK",
        from_status=row["status"],
        to_status=row["status"],
        detail=detail,
    )

    streak = healthy_check_count(connection, incident_id)
    if streak < REQUIRED_HEALTHY_CHECKS:
        LOGGER.info(
            "incident %d has %d/%d verified healthy checks; staying %s",
            incident_id,
            streak,
            REQUIRED_HEALTHY_CHECKS,
            row["status"],
        )
        return None

    execute(
        connection,
        """
        UPDATE incidents
        SET status = 'RESOLVED',
            resolved_at = now(),
            resolved_run_id = %s,
            suppressed_until = NULL,
            resolution_note = %s
        WHERE incident_id = %s
        """,
        (
            run_id,
            f"resolved after {streak} consecutive verified healthy measurements",
            incident_id,
        ),
    )
    _record_event(
        connection,
        incident_id,
        run_id=run_id,
        event_type="RESOLVED",
        from_status=row["status"],
        to_status="RESOLVED",
        detail=(
            f"{streak} consecutive verified healthy measurements; resolution is "
            "evidence-based, never inferred from an absence of alerts"
        ),
    )
    LOGGER.info("incident %d RESOLVED after %d healthy checks", incident_id, streak)
    return IncidentAction(
        incident_id=incident_id,
        fingerprint=fingerprint,
        dataset=dataset,
        action="RESOLVED",
        severity=Severity.INFO,
        should_alert=True,
        title=row["title"],
        detail=f"resolved after {streak} verified healthy measurements",
        occurrence_count=int(row["occurrence_count"]),
    )
