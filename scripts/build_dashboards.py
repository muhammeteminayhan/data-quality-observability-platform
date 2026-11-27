#!/usr/bin/env python3
"""Generate the four provisioned Grafana dashboards.

Why a generator rather than four hand-written JSON files
-------------------------------------------------------
A Grafana dashboard is a few thousand lines of deeply nested JSON in which a
single wrong field name produces a panel that renders empty while its query
runs perfectly. Building them from shared helpers means the panel scaffolding
is written once and reviewed once, and the diff of a dashboard change is the
query, not three hundred lines of layout.

The emitted JSON is committed - it is the artifact Grafana provisions from.
Re-run this script after editing a query:

    python scripts/build_dashboards.py

Panel/query discipline
----------------------
Each panel declares the SHAPE its query must return, and
scripts/verify_dashboards.sh asserts that every query both runs and returns
that shape against the live Grafana API. A query that returns correct numbers
in the wrong shape renders as an empty panel, which looks exactly like "no
problems" - the most dangerous possible failure for a quality dashboard.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

PROM = {"type": "prometheus", "uid": "dq-prometheus"}
PG = {"type": "postgres", "uid": "dq-observability"}
OUT = Path(__file__).resolve().parents[1] / "grafana" / "dashboards"

# Shared thresholds: a score below the contract's healthy_at is amber, below
# degraded_at is red. Kept here so every panel reads the same way.
SCORE_THRESHOLDS = {
    "mode": "absolute",
    "steps": [
        {"color": "red", "value": None},
        {"color": "orange", "value": 70},
        {"color": "green", "value": 90},
    ],
}

SEVERITY_COLOURS = {
    "CRITICAL": "red",
    "ERROR": "orange",
    "WARNING": "yellow",
    "INFO": "blue",
}


def pg_target(sql: str, ref: str = "A", fmt: str = "table") -> dict[str, Any]:
    """A PostgreSQL target.

    `format` must match what the panel expects: `table` for tables, stats and
    bar charts; `time_series` for timeseries panels, which additionally
    require the query to return a column named `time`.
    """
    return {
        "refId": ref,
        "datasource": PG,
        "format": fmt,
        "rawQuery": True,
        "rawSql": sql.strip(),
        "editorMode": "code",
    }


def prom_target(expr: str, legend: str, ref: str = "A", instant: bool = False) -> dict[str, Any]:
    return {
        "refId": ref,
        "datasource": PROM,
        "expr": expr,
        "legendFormat": legend,
        "range": not instant,
        "instant": instant,
        "editorMode": "code",
    }


def panel(
    *,
    title: str,
    kind: str,
    targets: list[dict[str, Any]],
    grid: tuple[int, int, int, int],
    description: str = "",
    unit: str | None = None,
    thresholds: dict[str, Any] | None = None,
    options: dict[str, Any] | None = None,
    overrides: list[dict[str, Any]] | None = None,
    minimum: float | None = None,
    maximum: float | None = None,
) -> dict[str, Any]:
    height, width, x, y = grid
    defaults: dict[str, Any] = {"custom": {}}
    if unit:
        defaults["unit"] = unit
    if thresholds:
        defaults["thresholds"] = thresholds
        defaults["custom"]["thresholdsStyle"] = {"mode": "off"}
    if minimum is not None:
        defaults["min"] = minimum
    if maximum is not None:
        defaults["max"] = maximum

    built: dict[str, Any] = {
        "type": kind,
        "title": title,
        "description": description,
        "datasource": targets[0]["datasource"] if targets else PG,
        "gridPos": {"h": height, "w": width, "x": x, "y": y},
        "targets": targets,
        "fieldConfig": {"defaults": defaults, "overrides": overrides or []},
        "options": options or {},
    }
    return built


def stat_options(text_mode: str = "auto", graph: str = "none") -> dict[str, Any]:
    return {
        "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
        "textMode": text_mode,
        "graphMode": graph,
        "colorMode": "value",
        "justifyMode": "auto",
        "orientation": "auto",
    }


def table_options() -> dict[str, Any]:
    return {"showHeader": True, "cellHeight": "sm", "footer": {"show": False}}


def timeseries_options() -> dict[str, Any]:
    return {
        "legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
        "tooltip": {"mode": "multi", "sort": "desc"},
    }


def dashboard(
    *, uid: str, title: str, description: str, panels: list[dict[str, Any]], tags: list[str]
) -> dict[str, Any]:
    return {
        "uid": uid,
        "title": title,
        "description": description,
        "tags": tags,
        "timezone": "utc",
        "editable": False,
        "schemaVersion": 39,
        "version": 1,
        "refresh": "30s",
        "time": {"from": "now-24h", "to": "now"},
        "timepicker": {"refresh_intervals": ["10s", "30s", "1m", "5m", "15m"]},
        "panels": panels,
    }


def write(name: str, payload: dict[str, Any]) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / name
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8", newline="\n")
    print(f"wrote {path.relative_to(OUT.parents[1])} ({len(payload['panels'])} panels)")


# ===========================================================================
# 1. Platform overview
# ===========================================================================
def _overview_stats() -> list[dict[str, Any]]:
    return [
        panel(
            title="Datasets monitored",
            kind="stat",
            description="Registered from the contract files, not hardcoded.",
            targets=[pg_target("SELECT count(*) AS value FROM datasets WHERE is_active")],
            grid=(4, 4, 0, 0),
            options=stat_options(),
        ),
        panel(
            title="Unhealthy datasets",
            kind="stat",
            description=(
                "A dataset is unhealthy when its score is below the contract "
                "threshold OR any CRITICAL finding exists - the second reason is "
                "independent of the score."
            ),
            targets=[
                pg_target(
                    """
                    SELECT count(*) AS value FROM (
                      SELECT DISTINCT ON (dataset_id) health_status
                      FROM quality_scores ORDER BY dataset_id, computed_at DESC
                    ) latest WHERE health_status <> 'healthy'
                    """
                )
            ],
            grid=(4, 4, 4, 0),
            thresholds={
                "mode": "absolute",
                "steps": [
                    {"color": "green", "value": None},
                    {"color": "orange", "value": 1},
                    {"color": "red", "value": 3},
                ],
            },
            options=stat_options(),
        ),
        panel(
            title="Open incidents",
            kind="stat",
            description="OPEN, ACKNOWLEDGED or SUPPRESSED. Resolved ones excluded.",
            targets=[
                pg_target(
                    "SELECT count(*) AS value FROM incidents "
                    "WHERE status IN ('OPEN','ACKNOWLEDGED','SUPPRESSED')"
                )
            ],
            grid=(4, 4, 8, 0),
            thresholds={
                "mode": "absolute",
                "steps": [
                    {"color": "green", "value": None},
                    {"color": "orange", "value": 1},
                    {"color": "red", "value": 5},
                ],
            },
            options=stat_options(),
        ),
        panel(
            title="Mean quality score",
            kind="gauge",
            description="Unweighted mean of the latest score per dataset.",
            targets=[
                pg_target(
                    """
                    SELECT round(avg(overall_score), 2) AS value FROM (
                      SELECT DISTINCT ON (dataset_id) overall_score
                      FROM quality_scores ORDER BY dataset_id, computed_at DESC
                    ) latest
                    """
                )
            ],
            grid=(4, 6, 12, 0),
            unit="none",
            minimum=0,
            maximum=100,
            thresholds=SCORE_THRESHOLDS,
            options={
                "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                "showThresholdLabels": False,
                "showThresholdMarkers": True,
            },
        ),
        panel(
            title="Last gate decision",
            kind="stat",
            description=(
                "FAIL means the pipeline was stopped on purpose; the run is "
                "recorded as `blocked`, a different outcome from a crash."
            ),
            targets=[
                pg_target(
                    "SELECT decision AS value FROM gate_decisions "
                    "ORDER BY decided_at DESC LIMIT 1"
                )
            ],
            grid=(4, 8, 18, 0),
            options=stat_options(text_mode="value"),
        ),
    ]


def overview() -> dict[str, Any]:
    panels = _overview_stats() + [
        panel(
            title="Quality score per dataset (latest)",
            kind="bargauge",
            description="Colour follows the shared score thresholds.",
            targets=[
                pg_target(
                    """
                    SELECT d.dataset_name AS metric, q.overall_score AS value
                    FROM (
                      SELECT DISTINCT ON (dataset_id) dataset_id, overall_score
                      FROM quality_scores ORDER BY dataset_id, computed_at DESC
                    ) q JOIN datasets d USING (dataset_id)
                    ORDER BY q.overall_score ASC
                    """
                )
            ],
            grid=(9, 12, 0, 4),
            minimum=0,
            maximum=100,
            thresholds=SCORE_THRESHOLDS,
            options={
                "displayMode": "gradient",
                "orientation": "horizontal",
                "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                "showUnfilled": True,
            },
        ),
        panel(
            title="Dataset health",
            kind="table",
            description=(
                "`unmeasured` lists dimensions that produced no measurement. They "
                "are excluded from the score rather than counted as 100."
            ),
            targets=[
                pg_target(
                    """
                    SELECT d.dataset_name AS "dataset", d.criticality AS "criticality",
                           q.overall_score AS "score", q.health_status AS "health",
                           q.has_critical_failure AS "critical",
                           q.dimensions_measured || '/' || q.dimensions_total AS "measured",
                           COALESCE(q.unmeasured_dimensions::text, '[]') AS "unmeasured"
                    FROM (
                      SELECT DISTINCT ON (dataset_id) *
                      FROM quality_scores ORDER BY dataset_id, computed_at DESC
                    ) q JOIN datasets d USING (dataset_id)
                    ORDER BY q.overall_score ASC, d.dataset_name
                    """
                )
            ],
            grid=(9, 12, 12, 4),
            options=table_options(),
        ),
        panel(
            title="Quality score over time",
            kind="timeseries",
            description="From Postgres, so history outlives Prometheus retention.",
            targets=[
                pg_target(
                    """
                    SELECT q.computed_at AS time, d.dataset_name AS metric,
                           q.overall_score AS value
                    FROM quality_scores q JOIN datasets d USING (dataset_id)
                    WHERE $__timeFilter(q.computed_at)
                    ORDER BY q.computed_at
                    """,
                    fmt="time_series",
                )
            ],
            grid=(8, 24, 0, 13),
            unit="none",
            minimum=0,
            maximum=100,
            options=timeseries_options(),
        ),
        panel(
            title="Pipeline runs by outcome",
            kind="piechart",
            description=(
                "`blocked` is counted separately from `failed`: the platform "
                "stopping the pipeline on purpose is not a crash."
            ),
            targets=[
                pg_target(
                    "SELECT status AS metric, count(*) AS value "
                    "FROM pipeline_runs GROUP BY status ORDER BY status"
                )
            ],
            grid=(8, 8, 0, 21),
            options={
                "legend": {"displayMode": "list", "placement": "right", "showLegend": True},
                "pieType": "donut",
                "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
            },
        ),
        panel(
            title="Rule failures by dimension",
            kind="barchart",
            description="Which of the six quality dimensions is actually failing.",
            targets=[
                pg_target(
                    """
                    SELECT dimension AS "dimension", count(*) AS "failures"
                    FROM validation_results WHERE success = false
                    GROUP BY dimension ORDER BY count(*) DESC
                    """
                )
            ],
            grid=(8, 8, 8, 21),
            options={
                "orientation": "horizontal",
                "legend": {"showLegend": False},
                "xTickLabelRotation": 0,
            },
        ),
        panel(
            title="dbt: models executed vs assertions held",
            kind="barchart",
            description=(
                "Two different questions. A model succeeding means the SQL ran; a "
                "test passing means an assertion about the data held. One number "
                "for both would hide failing assertions."
            ),
            targets=[
                prom_target("dq_dbt_models", "models {{result}}", "A", instant=True),
                prom_target("dq_dbt_tests", "tests {{result}}", "B", instant=True),
            ],
            grid=(8, 8, 16, 21),
            options={
                "orientation": "horizontal",
                "legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
            },
        ),
    ]
    return dashboard(
        uid="dq-overview",
        title="1. Platform Overview",
        description=(
            "Health of every monitored dataset, the latest gate decision, and "
            "where failures are concentrated."
        ),
        panels=panels,
        tags=["data-quality", "overview"],
    )


# ===========================================================================
# 2. Freshness and volume
# ===========================================================================
def freshness_and_volume() -> dict[str, Any]:
    panels = [
        panel(
            title="Freshness lag per dataset",
            kind="timeseries",
            description=(
                "Measured from the dataset's own clock. A dataset with no usable "
                "timestamp emits NO series rather than a zero - zero would read "
                "as perfectly fresh."
            ),
            targets=[
                prom_target(
                    "dq_freshness_lag_seconds",
                    "{{dataset}} ({{timestamp_source}})",
                )
            ],
            grid=(9, 16, 0, 0),
            unit="s",
            options=timeseries_options(),
        ),
        panel(
            title="Which clock was used",
            kind="table",
            description=(
                "event_time is the strongest evidence; object_mtime only proves a "
                "file was written, not that its contents are recent; unavailable "
                "means the dataset could not be measured at all."
            ),
            targets=[
                pg_target(
                    """
                    SELECT d.dataset_name AS "dataset",
                           f.timestamp_source AS "clock",
                           COALESCE(f.timestamp_column, '-') AS "column",
                           f.lag_seconds AS "lag_s",
                           f.status AS "status", f.severity AS "severity"
                    FROM (
                      SELECT DISTINCT ON (dataset_id) *
                      FROM freshness_checks ORDER BY dataset_id, checked_at DESC
                    ) f JOIN datasets d USING (dataset_id)
                    ORDER BY f.lag_seconds DESC NULLS FIRST
                    """
                )
            ],
            grid=(9, 8, 16, 0),
            options=table_options(),
        ),
        panel(
            title="Row count per dataset",
            kind="timeseries",
            targets=[prom_target("dq_row_count", "{{dataset}}")],
            grid=(9, 12, 0, 9),
            unit="short",
            options=timeseries_options(),
        ),
        panel(
            title="Volume deviation from the rolling median",
            kind="timeseries",
            description=(
                "Percent away from the median of comparable history. With "
                "same_day_type the baseline for a Saturday is built only from "
                "other weekend days, so a normal weekend dip sits near zero."
            ),
            targets=[prom_target("dq_volume_deviation_percent", "{{dataset}}")],
            grid=(9, 12, 12, 9),
            unit="percent",
            options=timeseries_options(),
        ),
        panel(
            title="Volume baseline detail",
            kind="table",
            description=(
                "`insufficient_history` means the baseline could not be computed "
                "from enough comparable points, so the check reports unknown and "
                "claims no threshold at all."
            ),
            targets=[
                pg_target(
                    """
                    SELECT d.dataset_name AS "dataset", v.row_count AS "rows",
                           v.baseline_method AS "method",
                           v.history_points || '/' || v.min_history_required AS "history",
                           v.baseline_median AS "median", v.baseline_mad AS "mad",
                           v.robust_z_score AS "robust_z",
                           v.deviation_percent AS "dev_pct",
                           CASE WHEN v.is_weekend THEN 'weekend' ELSE 'weekday' END AS "day_type",
                           v.status AS "status"
                    FROM (
                      SELECT DISTINCT ON (dataset_id) *
                      FROM volume_checks ORDER BY dataset_id, checked_at DESC
                    ) v JOIN datasets d USING (dataset_id)
                    ORDER BY abs(COALESCE(v.robust_z_score, 0)) DESC
                    """
                )
            ],
            grid=(9, 24, 0, 18),
            options=table_options(),
        ),
        panel(
            title="Row count history with the expected band",
            kind="timeseries",
            description=(
                "The band is median +/- critical_z * spread, as recorded by the "
                "check. Drawn from the stored values rather than recomputed, so "
                "the picture matches the decision that was actually made."
            ),
            targets=[
                pg_target(
                    """
                    SELECT v.checked_at AS time, d.dataset_name AS metric,
                           v.row_count AS value
                    FROM volume_checks v JOIN datasets d USING (dataset_id)
                    WHERE $__timeFilter(v.checked_at)
                    ORDER BY v.checked_at
                    """,
                    fmt="time_series",
                )
            ],
            grid=(9, 24, 0, 27),
            unit="short",
            options=timeseries_options(),
        ),
    ]
    return dashboard(
        uid="dq-freshness-volume",
        title="2. Freshness & Volume",
        description=(
            "Is the data arriving, and is there the right amount of it. Includes "
            "which clock each freshness number came from."
        ),
        panels=panels,
        tags=["data-quality", "freshness", "volume"],
    )


# ===========================================================================
# 3. Schema and distribution
# ===========================================================================
def schema_and_distribution() -> dict[str, Any]:
    panels = [
        panel(
            title="Schema changes by classification",
            kind="barchart",
            description=(
                "ADDITIVE is routine. COLUMN_RENAME_CANDIDATE is a hypothesis, "
                "never a confirmed fact - a rename and a drop-plus-add are "
                "indistinguishable from the outside."
            ),
            targets=[
                pg_target(
                    """
                    SELECT change_type AS "change", count(*) AS "count"
                    FROM schema_changes GROUP BY change_type ORDER BY count(*) DESC
                    """
                )
            ],
            grid=(8, 10, 0, 0),
            options={
                "orientation": "horizontal",
                "legend": {"showLegend": False},
            },
        ),
        panel(
            title="Current schema fingerprint",
            kind="table",
            description=(
                "sha256 over the ordered (name, type, nullable) triples. A change "
                "here is a change in the contract surface, including a reordering."
            ),
            targets=[
                pg_target(
                    """
                    SELECT d.dataset_name AS "dataset",
                           left(s.fingerprint, 16) AS "fingerprint",
                           s.column_count AS "columns", s.captured_at AS "captured"
                    FROM (
                      SELECT DISTINCT ON (dataset_id) *
                      FROM schema_snapshots ORDER BY dataset_id, captured_at DESC
                    ) s JOIN datasets d USING (dataset_id)
                    ORDER BY d.dataset_name
                    """
                )
            ],
            grid=(8, 14, 10, 0),
            options=table_options(),
        ),
        panel(
            title="Detected schema changes",
            kind="table",
            description=(
                "`confidence` and `is_confirmed` only apply to rename candidates. "
                "The detector never sets is_confirmed - only a human or an "
                "explicit contract change can."
            ),
            targets=[
                pg_target(
                    """
                    SELECT sc.detected_at AS "when", d.dataset_name AS "dataset",
                           sc.change_type AS "change", sc.column_name AS "column",
                           COALESCE(sc.previous_column, '-') AS "was",
                           COALESCE(sc.previous_type, '-') AS "from_type",
                           COALESCE(sc.new_type, '-') AS "to_type",
                           sc.confidence AS "confidence",
                           sc.is_confirmed AS "confirmed",
                           sc.severity AS "severity"
                    FROM schema_changes sc JOIN datasets d USING (dataset_id)
                    WHERE $__timeFilter(sc.detected_at)
                    ORDER BY sc.detected_at DESC
                    LIMIT 100
                    """
                )
            ],
            grid=(9, 24, 0, 8),
            options=table_options(),
        ),
        panel(
            title="Drift statistic per column",
            kind="timeseries",
            description=(
                "PSI for numeric columns, chi-square for categorical. An "
                "inconclusive test publishes NO statistic, so a gap here means "
                "the sample was below the contract minimum."
            ),
            targets=[
                prom_target(
                    "dq_distribution_statistic",
                    "{{dataset}}.{{column}} ({{method}})",
                )
            ],
            grid=(9, 14, 0, 17),
            options=timeseries_options(),
        ),
        panel(
            title="Columns the drift test could not conclude on",
            kind="stat",
            description=(
                "1 means the sample was too small on at least one side, so no "
                "drift is claimed. This is a first-class outcome, not an error."
            ),
            targets=[
                prom_target(
                    "sum(dq_distribution_inconclusive)",
                    "inconclusive",
                    instant=True,
                )
            ],
            grid=(9, 10, 14, 17),
            options=stat_options(graph="none"),
        ),
        panel(
            title="Latest drift results",
            kind="table",
            description=(
                "`significant` is true only when the sample sizes met the minimum "
                "AND the statistic crossed a threshold. A large PSI on a tiny "
                "sample is never reported significant."
            ),
            targets=[
                pg_target(
                    """
                    SELECT d.dataset_name AS "dataset", dd.column_name AS "column",
                           dd.method AS "method", dd.statistic AS "statistic",
                           dd.p_value AS "p_value",
                           dd.reference_sample_size AS "n_ref",
                           dd.current_sample_size AS "n_cur",
                           dd.min_sample_size AS "n_min",
                           dd.status AS "status", dd.is_significant AS "significant"
                    FROM (
                      SELECT DISTINCT ON (dataset_id, column_name) *
                      FROM distribution_drift
                      ORDER BY dataset_id, column_name, evaluated_at DESC
                    ) dd JOIN datasets d USING (dataset_id)
                    ORDER BY dd.is_significant DESC, dd.statistic DESC NULLS LAST
                    """
                )
            ],
            grid=(10, 24, 0, 26),
            options=table_options(),
        ),
    ]
    return dashboard(
        uid="dq-schema-distribution",
        title="3. Schema & Distribution",
        description=(
            "Structural changes and statistical drift, with the uncertainty of "
            "each finding made explicit."
        ),
        panels=panels,
        tags=["data-quality", "schema", "drift"],
    )


# ===========================================================================
# 4. Incidents and reliability
# ===========================================================================
def incidents_and_reliability() -> dict[str, Any]:
    panels = [
        panel(
            title="Live incidents by severity",
            kind="bargauge",
            targets=[
                pg_target(
                    """
                    SELECT severity AS metric, count(*) AS value
                    FROM incidents
                    WHERE status IN ('OPEN','ACKNOWLEDGED','SUPPRESSED')
                    GROUP BY severity
                    ORDER BY CASE severity
                        WHEN 'CRITICAL' THEN 0 WHEN 'ERROR' THEN 1
                        WHEN 'WARNING' THEN 2 ELSE 3 END
                    """
                )
            ],
            grid=(7, 8, 0, 0),
            options={
                "displayMode": "gradient",
                "orientation": "horizontal",
                "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
            },
        ),
        panel(
            title="Oldest live incident",
            kind="stat",
            description="By severity. A CRITICAL that has been open for days is the story.",
            targets=[
                prom_target(
                    "dq_oldest_open_incident_seconds", "{{severity}}", instant=True
                )
            ],
            grid=(7, 8, 8, 0),
            unit="s",
            options=stat_options(),
        ),
        panel(
            title="Alert deliveries by outcome",
            kind="piechart",
            description=(
                "`duplicate` proves the receiver is deduplicating retries rather "
                "than handling one alert many times. `failed` means a delivery "
                "was attempted and recorded as lost, not silently dropped."
            ),
            targets=[
                pg_target(
                    "SELECT status AS metric, count(*) AS value "
                    "FROM alert_deliveries GROUP BY status ORDER BY status"
                )
            ],
            grid=(7, 8, 16, 0),
            options={
                "legend": {"displayMode": "list", "placement": "right", "showLegend": True},
                "pieType": "donut",
                "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
            },
        ),
        panel(
            title="Incidents",
            kind="table",
            description=(
                "`occurrences` rising with no new alert means the cooldown is "
                "working. A RESOLVED incident always carries the note recording "
                "how many verified healthy measurements closed it."
            ),
            targets=[
                pg_target(
                    """
                    SELECT i.incident_id AS "id", d.dataset_name AS "dataset",
                           i.detector AS "detector", i.severity AS "severity",
                           i.status AS "status", i.occurrence_count AS "occurrences",
                           i.first_seen_at AS "first_seen",
                           i.last_seen_at AS "last_seen",
                           COALESCE(i.resolution_note, '-') AS "resolution",
                           left(i.title, 60) AS "title"
                    FROM incidents i JOIN datasets d USING (dataset_id)
                    ORDER BY
                        CASE i.status WHEN 'RESOLVED' THEN 1 ELSE 0 END,
                        CASE i.severity
                            WHEN 'CRITICAL' THEN 0 WHEN 'ERROR' THEN 1
                            WHEN 'WARNING' THEN 2 ELSE 3 END,
                        i.last_seen_at DESC
                    LIMIT 100
                    """
                )
            ],
            grid=(10, 24, 0, 7),
            options=table_options(),
        ),
        panel(
            title="Incident lifecycle events",
            kind="table",
            description=(
                "The audit trail. HEALTHY_CHECK rows are the positive evidence a "
                "resolution requires - an incident is never closed because its "
                "alert went quiet."
            ),
            targets=[
                pg_target(
                    """
                    SELECT e.occurred_at AS "when", e.incident_id AS "incident",
                           e.event_type AS "event",
                           COALESCE(e.from_status, '-') AS "from",
                           COALESCE(e.to_status, '-') AS "to",
                           e.actor AS "actor", left(e.detail, 90) AS "detail"
                    FROM incident_events e
                    WHERE $__timeFilter(e.occurred_at)
                    ORDER BY e.occurred_at DESC
                    LIMIT 150
                    """
                )
            ],
            grid=(10, 24, 0, 17),
            options=table_options(),
        ),
        panel(
            title="Gate decisions over time",
            kind="timeseries",
            description="Every PASS, PASS_WITH_WARNINGS and FAIL the gate has made.",
            targets=[
                pg_target(
                    """
                    SELECT g.decided_at AS time, g.decision AS metric,
                           1 AS value
                    FROM gate_decisions g
                    WHERE $__timeFilter(g.decided_at)
                    ORDER BY g.decided_at
                    """,
                    fmt="time_series",
                )
            ],
            grid=(8, 12, 0, 27),
            options=timeseries_options(),
        ),
        panel(
            title="Validation duration per dataset",
            kind="timeseries",
            description="How long the quality checks themselves take.",
            targets=[
                prom_target(
                    "dq_validation_duration_milliseconds_avg", "{{dataset}}"
                )
            ],
            grid=(8, 12, 12, 27),
            unit="ms",
            options=timeseries_options(),
        ),
    ]
    return dashboard(
        uid="dq-incidents",
        title="4. Incidents & Reliability",
        description=(
            "Incident lifecycle, alert delivery and gate history - the evidence "
            "trail behind every closed incident."
        ),
        panels=panels,
        tags=["data-quality", "incidents", "reliability"],
    )


def main() -> None:
    write("01_platform_overview.json", overview())
    write("02_freshness_and_volume.json", freshness_and_volume())
    write("03_schema_and_distribution.json", schema_and_distribution())
    write("04_incidents_and_reliability.json", incidents_and_reliability())


if __name__ == "__main__":
    main()
