"""Alert webhook receiver.

    uvicorn src.webhook.receiver:app --host 0.0.0.0 --port 8000

Accepts alerts from Grafana (and from the platform itself), validates their
structure, records every delivery, and refuses to handle the same alert twice.

Needs no external service and no API key, which is the point: the demo has to
work offline. Grafana posts here, this process records the delivery in
`alert_deliveries`, and the API and dashboards read it back - so "an alert was
actually delivered" is a queryable fact rather than an assumption.

Idempotency
-----------
Alerting systems retry. Grafana will re-POST a firing alert on its own
schedule, and a receiver that handled each delivery as new would multiply one
problem into dozens of records. Every request is fingerprinted over the fields
that identify the ALERT rather than the delivery (so a retry matches, while a
genuinely new firing does not), and a repeat is stored with
`status='duplicate'` and acknowledged with 200 - not 4xx, because the sender
did nothing wrong and a non-2xx would make it retry harder.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any, Literal

from fastapi import FastAPI, Header, HTTPException, Request, status
from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest
from pydantic import BaseModel, Field, field_validator
from starlette.responses import JSONResponse, Response

from src.common.config import get_settings
from src.common.db import execute, fetch_all, fetch_one, observability_connection, ping
from src.common.logging import get_logger

LOGGER = get_logger(__name__)

app = FastAPI(
    title="Data Quality Alert Receiver",
    version="1.0.0",
    description=(
        "Receives data-quality alerts, validates them, records every delivery "
        "and deduplicates retries."
    ),
)

ALERTS_RECEIVED = Counter(
    "dq_webhook_alerts_received",
    "Alert deliveries received, by outcome",
    ["outcome"],
)


class AlertPayload(BaseModel):
    """The structure this receiver accepts.

    Deliberately strict about the few fields that matter and tolerant about
    everything else: Grafana's payload shape varies by version and by
    notification template, so rejecting unknown keys would make the receiver
    brittle for no safety gain. The fields below are the ones recorded.
    """

    status: Literal["firing", "resolved"] = "firing"
    dataset: str | None = None
    rule: str | None = None
    severity: str | None = None
    title: str = Field(default="", max_length=500)
    message: str = Field(default="", max_length=4000)
    incident_id: int | None = None
    # Grafana nests the real content; kept whole so nothing is lost.
    alerts: list[dict[str, Any]] = Field(default_factory=list)
    labels: dict[str, Any] = Field(default_factory=dict)
    annotations: dict[str, Any] = Field(default_factory=dict)
    externalURL: str | None = None
    fingerprint: str | None = None

    @field_validator("severity")
    @classmethod
    def _normalise_severity(cls, value: str | None) -> str | None:
        if value is None:
            return None
        upper = value.strip().upper()
        if upper not in {"INFO", "WARNING", "ERROR", "CRITICAL"}:
            # Recorded as-is rather than rejected: an unknown severity from an
            # external sender is information, not a reason to drop the alert.
            LOGGER.warning("unrecognised severity %r in an incoming alert", value)
        return upper


def alert_fingerprint(payload: AlertPayload, raw_body: bytes) -> str:
    """Identity of the ALERT, not of the delivery.

    A retry of the same firing must produce the same value, while a new
    firing must not. So the fingerprint is taken over the identifying fields
    only - never over a timestamp or a delivery id, which would make every
    retry look unique and defeat the deduplication entirely.

    Grafana supplies its own stable `fingerprint`; it is preferred when
    present because it is exactly this concept, computed by the sender.
    """
    if payload.fingerprint:
        return hashlib.sha256(
            f"grafana:{payload.fingerprint}:{payload.status}".encode()
        ).hexdigest()

    parts = [
        payload.status,
        payload.dataset or "-",
        payload.rule or "-",
        payload.severity or "-",
        payload.title,
        str(payload.incident_id or "-"),
    ]
    if not any(part not in {"-", "", "firing", "resolved"} for part in parts):
        # Nothing identifying at all: fall back to the body so two different
        # anonymous alerts are still told apart.
        return hashlib.sha256(raw_body).hexdigest()
    return hashlib.sha256("|".join(parts).encode()).hexdigest()


def _require_token(provided: str | None) -> None:
    """Check the shared secret, when one is configured.

    A local portfolio deployment with no token set stays open so the demo
    works out of the box; that trade-off is recorded in docs/SECURITY.md
    rather than left implicit.
    """
    expected = get_settings().alerting.webhook_token
    if not expected:
        return
    if provided != expected:
        ALERTS_RECEIVED.labels(outcome="rejected").inc()
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing or incorrect X-DQ-Token",
        )


@app.get("/health")
def health() -> dict[str, Any]:
    """Liveness plus a real dependency check."""
    database_ok = ping(get_settings().observability)
    return {
        "status": "ok" if database_ok else "degraded",
        "observability_db": "reachable" if database_ok else "unreachable",
        "checked_at": datetime.now(UTC).isoformat(),
    }


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/alerts")
async def receive_alert(
    request: Request,
    x_dq_token: str | None = Header(default=None, alias="X-DQ-Token"),
    authorization: str | None = Header(default=None),
) -> JSONResponse:
    """Accept, validate, deduplicate and record one alert delivery.

    The token may arrive either in X-DQ-Token (what the platform itself uses)
    or as `Authorization: Bearer <token>`. Grafana's webhook receiver sends
    credentials as an Authorization header with a scheme, not as an arbitrary
    custom header, so supporting both is what lets one receiver serve both
    senders without weakening the check.
    """
    bearer = None
    if authorization and authorization.lower().startswith("bearer "):
        bearer = authorization.split(None, 1)[1].strip()
    _require_token(x_dq_token or bearer)

    raw_body = await request.body()
    try:
        parsed = AlertPayload.model_validate_json(raw_body or b"{}")
    except Exception as exc:
        # A malformed alert is recorded as `rejected` rather than discarded:
        # a sender that has started posting rubbish is itself an incident, and
        # it would otherwise be invisible.
        ALERTS_RECEIVED.labels(outcome="rejected").inc()
        LOGGER.warning("rejected a malformed alert: %s", exc)
        with observability_connection() as connection:
            execute(
                connection,
                """
                INSERT INTO alert_deliveries (
                    channel, request_fingerprint, payload, status, error_message
                )
                VALUES ('webhook', %s, %s, 'rejected', %s)
                ON CONFLICT (channel, request_fingerprint) DO NOTHING
                """,
                (
                    hashlib.sha256(raw_body).hexdigest(),
                    json.dumps({"raw": raw_body.decode("utf-8", "replace")[:4000]}),
                    str(exc)[:500],
                ),
            )
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"alert payload could not be parsed: {exc}",
        ) from exc

    fingerprint = alert_fingerprint(parsed, raw_body)

    with observability_connection() as connection:
        existing = fetch_one(
            connection,
            """
            SELECT alert_delivery_id, status
            FROM alert_deliveries
            WHERE channel = 'webhook' AND request_fingerprint = %s
            """,
            (fingerprint,),
        )

        if existing is not None:
            ALERTS_RECEIVED.labels(outcome="duplicate").inc()
            LOGGER.info(
                "duplicate alert %s (already delivery %s)",
                fingerprint[:12],
                existing["alert_delivery_id"],
            )
            # 200, not 409: the sender did nothing wrong, and a non-2xx would
            # make it retry harder and faster.
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content={
                    "result": "duplicate",
                    "fingerprint": fingerprint,
                    "original_delivery_id": existing["alert_delivery_id"],
                    "detail": "this alert was already handled; nothing was done again",
                },
            )

        execute(
            connection,
            """
            INSERT INTO alert_deliveries (
                incident_id, channel, external_id, request_fingerprint,
                payload, status, http_status
            )
            VALUES (%s, 'webhook', %s, %s, %s, 'received', 200)
            """,
            (
                parsed.incident_id,
                parsed.fingerprint,
                fingerprint,
                json.dumps(parsed.model_dump(), default=str),
            ),
        )

    ALERTS_RECEIVED.labels(outcome="received").inc()
    LOGGER.info(
        "alert received: status=%s dataset=%s severity=%s rule=%s fp=%s",
        parsed.status,
        parsed.dataset,
        parsed.severity,
        parsed.rule,
        fingerprint[:12],
    )
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={"result": "received", "fingerprint": fingerprint},
    )


@app.get("/alerts")
def list_alerts(limit: int = 50) -> dict[str, Any]:
    """Recent deliveries, so the demo can prove alerts actually arrived."""
    with observability_connection() as connection:
        rows = fetch_all(
            connection,
            """
            SELECT alert_delivery_id, incident_id, channel, status,
                   http_status, received_at,
                   payload -> 'dataset'  AS dataset,
                   payload -> 'severity' AS severity,
                   payload -> 'title'    AS title
            FROM alert_deliveries
            ORDER BY received_at DESC
            LIMIT %s
            """,
            (min(max(limit, 1), 500),),
        )
    return {"count": len(rows), "deliveries": rows}
