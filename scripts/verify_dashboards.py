#!/usr/bin/env python3
"""Assert every dashboard panel's query runs AND returns the right shape.

    python scripts/verify_dashboards.py

Why this exists
---------------
A Grafana panel whose query is correct but whose RESULT SHAPE is wrong renders
empty. An empty panel on a data-quality dashboard looks exactly like "no
problems found", which is the most dangerous possible failure: the dashboard
reassures you precisely when it is broken.

Running the query through the Grafana API is the only way to know. Reading the
JSON proves nothing, and looking at a screenshot proves only that one panel
looked plausible at one moment.

What each outcome means
-----------------------
    OK          the query ran and returned fields of the shape the panel needs
    EMPTY       the query ran and returned no rows. Reported, not failed:
                "no schema changes yet" is a legitimate empty result. The
                summary lists them so a human can judge each one.
    SHAPE       the query ran but the fields cannot drive this panel type -
                for example a timeseries panel whose query returns no time
                field. This is a FAILURE.
    ERROR       the query itself failed. A FAILURE.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from base64 import b64encode
from pathlib import Path
from typing import Any

GRAFANA = os.environ.get("GRAFANA_URL", "http://localhost:3046")
USER = os.environ.get("GRAFANA_ADMIN_USER", "admin")
PASSWORD = os.environ.get("GRAFANA_ADMIN_PASSWORD", "")
DASHBOARDS = Path(__file__).resolve().parents[1] / "grafana" / "dashboards"

# Panel types that need a time field in the result.
TIME_PANELS = {"timeseries", "state-timeline", "heatmap"}
# Panel types that need at least one numeric field.
NUMERIC_PANELS = {"stat", "gauge", "bargauge", "piechart", "barchart", "timeseries"}


def _request(path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    url = f"{GRAFANA}{path}"
    data = json.dumps(payload).encode() if payload is not None else None
    request = urllib.request.Request(url, data=data, method="POST" if data else "GET")  # noqa: S310
    token = b64encode(f"{USER}:{PASSWORD}".encode()).decode()
    request.add_header("Authorization", f"Basic {token}")
    request.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(request, timeout=45) as response:  # noqa: S310
            return json.loads(response.read().decode())
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", "replace")[:300]
        return {"__http_error__": f"{exc.code}: {body}"}
    except Exception as exc:
        return {"__http_error__": str(exc)}


def _expand_time_filter(sql: str) -> str:
    """Stand in for Grafana's $__timeFilter macro.

    Grafana expands `$__timeFilter(col)` to a BETWEEN over the dashboard's
    time range. Only Grafana does that, and only for a real dashboard
    request, so the macro has to be expanded here too - as a BOOLEAN
    predicate on the column, which is what it is.
    """
    import re

    pattern = r"\$__timeFilter\(\s*([^)]+?)\s*\)"
    return re.sub(pattern, lambda m: f"{m.group(1)} >= now() - interval '30 days'", sql)


def run_target(target: dict[str, Any]) -> tuple[str, list[dict[str, Any]], str]:
    """Execute one panel target. Returns (status, fields, detail)."""
    datasource = target.get("datasource") or {}
    query: dict[str, Any] = {
        "refId": target.get("refId", "A"),
        "datasource": datasource,
        "intervalMs": 60000,
        "maxDataPoints": 500,
    }
    if datasource.get("type") == "prometheus":
        query["expr"] = target["expr"]
        query["instant"] = bool(target.get("instant"))
        query["range"] = not query["instant"]
    else:
        # Grafana replaces the macro itself only for real dashboard requests,
        # so it is expanded here to a window wide enough to cover the data.
        sql = _expand_time_filter(target["rawSql"])
        query["rawSql"] = sql
        query["rawQuery"] = True
        query["format"] = target.get("format", "table")

    import time as _time

    now_ms = int(_time.time() * 1000)
    payload = {
        "queries": [query],
        # Epoch milliseconds rather than "now-7d": the relative form is
        # resolved by the dashboard front end, and /api/ds/query returned
        # empty frames for every Prometheus RANGE query when given it.
        "from": str(now_ms - 6 * 60 * 60 * 1000),
        "to": str(now_ms),
    }
    response = _request("/api/ds/query", payload)
    if "__http_error__" in response:
        return "ERROR", [], response["__http_error__"]

    results = response.get("results") or {}
    entry = results.get(query["refId"]) or {}
    if entry.get("error"):
        return "ERROR", [], str(entry["error"])[:220]

    frames = entry.get("frames") or []
    fields: list[dict[str, Any]] = []
    rows = 0
    for frame in frames:
        schema = frame.get("schema") or {}
        for field in schema.get("fields") or []:
            fields.append({"name": field.get("name"), "type": field.get("type")})
        data = frame.get("data") or {}
        values = data.get("values") or []
        if values:
            rows = max(rows, len(values[0]) if values[0] is not None else 0)

    if not fields:
        return "EMPTY", [], "query returned no fields at all"
    if rows == 0:
        return "EMPTY", fields, f"0 rows; fields={[f['name'] for f in fields]}"
    return "OK", fields, f"{rows} row(s); fields={[f['name'] for f in fields]}"


def check_shape(
    kind: str, fields: list[dict[str, Any]], options: dict[str, Any] | None = None
) -> str | None:
    """Return a reason the fields cannot drive this panel type, or None."""
    types = {str(f.get("type")) for f in fields}
    names = {str(f.get("name")) for f in fields}
    options = options or {}

    # A stat panel in text mode renders a string value, which is exactly what
    # "Last gate decision" (PASS / PASS_WITH_WARNINGS / FAIL) needs.
    if kind == "stat" and options.get("textMode") in {"value", "name"}:
        if types & {"string", "number", "int64", "float64"}:
            return None
        return f"stat in text mode needs a string or numeric field; got {sorted(types)}"

    if kind in TIME_PANELS:
        if "time" not in types and "time" not in names:
            return (
                f"{kind} needs a time field; got {sorted(names)} of types "
                f"{sorted(types)}"
            )
    if kind in NUMERIC_PANELS and kind not in TIME_PANELS:
        if not (types & {"number", "int64", "float64"}):
            return f"{kind} needs a numeric field; got types {sorted(types)}"
    if kind == "table" and not fields:
        return "table needs at least one field"
    return None


def main() -> int:
    if not PASSWORD:
        print(
            "GRAFANA_ADMIN_PASSWORD is not set. Export it from .env, e.g.\n"
            "  export GRAFANA_ADMIN_PASSWORD=$(grep ^GRAFANA_ADMIN_PASSWORD= .env | cut -d= -f2)",
            file=sys.stderr,
        )
        return 2

    files = sorted(DASHBOARDS.glob("*.json"))
    if not files:
        print(f"no dashboards found in {DASHBOARDS}", file=sys.stderr)
        return 2

    totals = {"OK": 0, "EMPTY": 0, "SHAPE": 0, "ERROR": 0}
    empties: list[str] = []
    failures: list[str] = []

    for path in files:
        payload = json.loads(path.read_text(encoding="utf-8"))
        print(f"\n=== {payload['title']} ({path.name}) ===")
        for panel in payload.get("panels", []):
            kind = panel.get("type", "?")
            title = panel.get("title", "(untitled)")
            targets = panel.get("targets") or []
            if not targets:
                print(f"  --   {title}: no targets")
                continue

            statuses: list[str] = []
            details: list[str] = []
            for target in targets:
                status, fields, detail = run_target(target)
                if status == "OK":
                    reason = check_shape(kind, fields, panel.get("options"))
                    if reason:
                        status, detail = "SHAPE", reason
                statuses.append(status)
                details.append(detail)

            worst = (
                "ERROR"
                if "ERROR" in statuses
                else "SHAPE"
                if "SHAPE" in statuses
                else "EMPTY"
                if "EMPTY" in statuses
                else "OK"
            )
            totals[worst] += 1
            marker = {"OK": "OK  ", "EMPTY": "EMPTY", "SHAPE": "SHAPE", "ERROR": "ERROR"}[worst]
            print(f"  {marker} [{kind}] {title}")
            if worst != "OK":
                for detail in details:
                    print(f"         {detail}")
                line = f"{payload['title']} / {title} [{kind}]: {details[0]}"
                (empties if worst == "EMPTY" else failures).append(line)

    print("\n" + "=" * 72)
    print(
        f"panels: {totals['OK']} ok, {totals['EMPTY']} empty, "
        f"{totals['SHAPE']} wrong shape, {totals['ERROR']} errored"
    )
    if empties:
        print("\nEmpty panels (query ran, no rows - judge each one):")
        for line in empties:
            print(f"  - {line}")
    if failures:
        print("\nFAILURES:")
        for line in failures:
            print(f"  - {line}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
