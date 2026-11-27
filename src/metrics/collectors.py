"""Prometheus collectors backed by observability_db.

Implemented as a custom Collector rather than module-level Gauges that
something increments. Two reasons:

* a Gauge set during a pipeline run keeps its last value forever, so a dataset
  that stopped being checked would keep reporting its final healthy score.
  Reading the database at scrape time means a missing measurement shows up as
  a missing series, which is what Prometheus absent() is for.
* the exporter is a separate process from the DAG, so it has no in-memory
  state to increment in the first place.

Label discipline
----------------
Every label here is drawn from a BOUNDED set: dataset names come from the
contract files, dimensions from the six fixed dimensions, severities from the
four levels, change types from the five classifications, column names from the
contracts drift specs. There is no label for a customer id, an order id, a run
id, an incident id, or an error message - each of those is unbounded and would
multiply the series count without limit.

Per-run and per-incident detail lives in observability_db, which is built for
it, and is read by the API and by Grafana Postgres datasource.
"""

from __future__ import annotations

from collections.abc import Iterable

from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily, Metric
from prometheus_client.registry import Collector

from src.common.db import fetch_all, observability_connection
from src.common.logging import get_logger

LOGGER = get_logger(__name__)

PREFIX = "dq"


class DataQualityCollector(Collector):
    """Exposes the platform measurements as Prometheus metrics."""

    def collect(self) -> Iterable[Metric]:
        try:
            with observability_connection() as connection:
                yield from self._quality_scores(connection)
                yield from self._freshness(connection)
                yield from self._volume(connection)
                yield from self._validation(connection)
                yield from self._rule_failures(connection)
                yield from self._schema_changes(connection)
                yield from self._distribution(connection)
                yield from self._incidents(connection)
                yield from self._gates(connection)
                yield from self._dbt(connection)
                yield from self._runs(connection)
        except Exception as exc:
            # A failed scrape is reported as a metric rather than an
            # exception, so Prometheus records "the exporter could not read
            # the database" instead of just failing the target with no detail.
            LOGGER.error("metric collection failed: %s", exc)
            failure = GaugeMetricFamily(
                f"{PREFIX}_collection_failed",
                "1 when the exporter could not read observability_db",
            )
            failure.add_metric([], 1.0)
            yield failure
            return

        ok = GaugeMetricFamily(
            f"{PREFIX}_collection_failed",
            "1 when the exporter could not read observability_db",
        )
        ok.add_metric([], 0.0)
        yield ok

    def _quality_scores(self, connection) -> Iterable[Metric]:
        rows = fetch_all(
            connection,
            """
            SELECT DISTINCT ON (d.dataset_name)
                d.dataset_name, q.overall_score, q.health_status,
                q.has_critical_failure, q.dimensions_measured
            FROM quality_scores q
            JOIN datasets d USING (dataset_id)
            ORDER BY d.dataset_name, q.computed_at DESC
            """,
        )
        score = GaugeMetricFamily(
            f"{PREFIX}_quality_score",
            "Latest weighted quality score, 0-100",
            labels=["dataset", "health"],
        )
        critical = GaugeMetricFamily(
            f"{PREFIX}_dataset_has_critical_failure",
            "1 when the dataset has a CRITICAL finding; it can never be healthy",
            labels=["dataset"],
        )
        measured = GaugeMetricFamily(
            f"{PREFIX}_dimensions_measured",
            "How many of the six quality dimensions produced a measurement",
            labels=["dataset"],
        )
        for row in rows:
            score.add_metric(
                [row["dataset_name"], row["health_status"]],
                float(row["overall_score"]),
            )
            critical.add_metric(
                [row["dataset_name"]], 1.0 if row["has_critical_failure"] else 0.0
            )
            measured.add_metric([row["dataset_name"]], float(row["dimensions_measured"]))
        yield score
        yield critical
        yield measured

    def _freshness(self, connection) -> Iterable[Metric]:
        rows = fetch_all(
            connection,
            """
            SELECT DISTINCT ON (d.dataset_name)
                d.dataset_name, f.lag_seconds, f.timestamp_source
            FROM freshness_checks f
            JOIN datasets d USING (dataset_id)
            ORDER BY d.dataset_name, f.checked_at DESC
            """,
        )
        lag = GaugeMetricFamily(
            f"{PREFIX}_freshness_lag_seconds",
            "Age of the newest data, by the clock named in timestamp_source",
            labels=["dataset", "timestamp_source"],
        )
        for row in rows:
            # A dataset with no usable timestamp emits NO series rather than a
            # zero. Zero would read as perfectly fresh.
            if row["lag_seconds"] is None:
                continue
            lag.add_metric(
                [row["dataset_name"], row["timestamp_source"]], float(row["lag_seconds"])
            )
        yield lag

    def _volume(self, connection) -> Iterable[Metric]:
        rows = fetch_all(
            connection,
            """
            SELECT DISTINCT ON (d.dataset_name)
                d.dataset_name, v.row_count, v.deviation_percent, v.robust_z_score
            FROM volume_checks v
            JOIN datasets d USING (dataset_id)
            ORDER BY d.dataset_name, v.checked_at DESC
            """,
        )
        rowcount = GaugeMetricFamily(
            f"{PREFIX}_row_count",
            "Rows observed in the dataset at the last check",
            labels=["dataset"],
        )
        deviation = GaugeMetricFamily(
            f"{PREFIX}_volume_deviation_percent",
            "Deviation from the rolling median row count, percent",
            labels=["dataset"],
        )
        zscore = GaugeMetricFamily(
            f"{PREFIX}_volume_robust_z_score",
            "Robust z-score of the row count against its MAD baseline",
            labels=["dataset"],
        )
        for row in rows:
            rowcount.add_metric([row["dataset_name"]], float(row["row_count"]))
            if row["deviation_percent"] is not None:
                deviation.add_metric([row["dataset_name"]], float(row["deviation_percent"]))
            if row["robust_z_score"] is not None:
                zscore.add_metric([row["dataset_name"]], float(row["robust_z_score"]))
        yield rowcount
        yield deviation
        yield zscore

    def _validation(self, connection) -> Iterable[Metric]:
        rows = fetch_all(
            connection,
            """
            SELECT d.dataset_name, vr.success, count(*) AS runs,
                   sum(vr.expectations_total) AS expectations,
                   avg(vr.duration_ms) AS avg_ms
            FROM validation_runs vr
            JOIN datasets d USING (dataset_id)
            GROUP BY d.dataset_name, vr.success
            """,
        )
        runs = CounterMetricFamily(
            f"{PREFIX}_validation_runs",
            "Validation runs executed, by outcome",
            labels=["dataset", "result"],
        )
        duration = GaugeMetricFamily(
            f"{PREFIX}_validation_duration_milliseconds_avg",
            "Mean validation duration per dataset",
            labels=["dataset"],
        )
        seen: set[str] = set()
        for row in rows:
            result = "success" if row["success"] else "failure"
            runs.add_metric([row["dataset_name"], result], float(row["runs"]))
            if row["dataset_name"] not in seen and row["avg_ms"] is not None:
                duration.add_metric([row["dataset_name"]], float(row["avg_ms"]))
                seen.add(row["dataset_name"])
        yield runs
        yield duration

    def _rule_failures(self, connection) -> Iterable[Metric]:
        rows = fetch_all(
            connection,
            """
            SELECT d.dataset_name, vr.dimension, vr.severity, count(*) AS n
            FROM validation_results vr
            JOIN datasets d USING (dataset_id)
            WHERE vr.success = false
            GROUP BY d.dataset_name, vr.dimension, vr.severity
            """,
        )
        failures = CounterMetricFamily(
            f"{PREFIX}_rule_failures",
            "Failed contract rules, by dimension and severity",
            labels=["dataset", "dimension", "severity"],
        )
        for row in rows:
            failures.add_metric(
                [row["dataset_name"], row["dimension"], row["severity"]],
                float(row["n"]),
            )
        yield failures

    def _schema_changes(self, connection) -> Iterable[Metric]:
        rows = fetch_all(
            connection,
            """
            SELECT d.dataset_name, sc.change_type, count(*) AS n
            FROM schema_changes sc
            JOIN datasets d USING (dataset_id)
            GROUP BY d.dataset_name, sc.change_type
            """,
        )
        changes = CounterMetricFamily(
            f"{PREFIX}_schema_changes",
            "Schema changes detected, by classification",
            labels=["dataset", "change_type"],
        )
        for row in rows:
            changes.add_metric(
                [row["dataset_name"], row["change_type"]], float(row["n"])
            )
        yield changes

    def _distribution(self, connection) -> Iterable[Metric]:
        rows = fetch_all(
            connection,
            """
            SELECT DISTINCT ON (d.dataset_name, dd.column_name)
                d.dataset_name, dd.column_name, dd.method, dd.statistic, dd.status
            FROM distribution_drift dd
            JOIN datasets d USING (dataset_id)
            ORDER BY d.dataset_name, dd.column_name, dd.evaluated_at DESC
            """,
        )
        statistic = GaugeMetricFamily(
            f"{PREFIX}_distribution_statistic",
            "Latest drift statistic (PSI or chi-square) per monitored column",
            labels=["dataset", "column", "method"],
        )
        inconclusive = GaugeMetricFamily(
            f"{PREFIX}_distribution_inconclusive",
            "1 when the drift test could not be run because the sample was too small",
            labels=["dataset", "column"],
        )
        for row in rows:
            # An inconclusive test publishes NO statistic. Publishing one
            # would invite a dashboard to plot a number the platform has
            # explicitly refused to stand behind.
            if row["status"] == "inconclusive" or row["statistic"] is None:
                inconclusive.add_metric([row["dataset_name"], row["column_name"]], 1.0)
                continue
            inconclusive.add_metric([row["dataset_name"], row["column_name"]], 0.0)
            statistic.add_metric(
                [row["dataset_name"], row["column_name"], row["method"]],
                float(row["statistic"]),
            )
        yield statistic
        yield inconclusive

    def _incidents(self, connection) -> Iterable[Metric]:
        rows = fetch_all(
            connection,
            """
            SELECT status, severity, count(*) AS n
            FROM incidents
            GROUP BY status, severity
            """,
        )
        # Labelled by status and severity only - NOT by incident id, which is
        # unbounded and would create a new series for every problem ever seen.
        incidents = GaugeMetricFamily(
            f"{PREFIX}_incidents",
            "Incidents by lifecycle status and severity",
            labels=["status", "severity"],
        )
        for row in rows:
            incidents.add_metric(
                [row["status"], row["severity"]], float(row["n"])
            )
        yield incidents

        oldest = fetch_all(
            connection,
            """
            SELECT severity,
                   max(extract(epoch from (now() - first_seen_at))) AS age_seconds
            FROM incidents
            WHERE status IN ('OPEN', 'ACKNOWLEDGED', 'SUPPRESSED')
            GROUP BY severity
            """,
        )
        age = GaugeMetricFamily(
            f"{PREFIX}_oldest_open_incident_seconds",
            "Age of the oldest live incident, by severity",
            labels=["severity"],
        )
        for row in oldest:
            if row["age_seconds"] is not None:
                age.add_metric([row["severity"]], float(row["age_seconds"]))
        yield age

    def _gates(self, connection) -> Iterable[Metric]:
        rows = fetch_all(
            connection,
            "SELECT decision, count(*) AS n FROM gate_decisions GROUP BY decision",
        )
        gates = CounterMetricFamily(
            f"{PREFIX}_gate_decisions",
            "Quality gate decisions",
            labels=["decision"],
        )
        for row in rows:
            gates.add_metric([row["decision"]], float(row["n"]))
        yield gates

    def _dbt(self, connection) -> Iterable[Metric]:
        row = fetch_all(
            connection,
            """
            SELECT models_total, models_success, models_error,
                   tests_total, tests_pass, tests_fail, tests_warn
            FROM dbt_runs
            ORDER BY dbt_run_id DESC
            LIMIT 1
            """,
        )
        # Models and tests are exposed as SEPARATE metrics on purpose. A model
        # succeeding means the SQL ran; a test passing means an assertion about
        # the data held. One dashboard panel showing "dbt: 12/12 green" from
        # the model counts alone would hide failing assertions entirely.
        models = GaugeMetricFamily(
            f"{PREFIX}_dbt_models",
            "Models in the last dbt invocation, by outcome (SQL execution)",
            labels=["result"],
        )
        tests = GaugeMetricFamily(
            f"{PREFIX}_dbt_tests",
            "Tests in the last dbt invocation, by outcome (data assertions)",
            labels=["result"],
        )
        if row:
            latest = row[0]
            models.add_metric(["success"], float(latest["models_success"]))
            models.add_metric(["error"], float(latest["models_error"]))
            tests.add_metric(["pass"], float(latest["tests_pass"]))
            tests.add_metric(["fail"], float(latest["tests_fail"]))
            tests.add_metric(["warn"], float(latest["tests_warn"]))
        yield models
        yield tests

    def _runs(self, connection) -> Iterable[Metric]:
        rows = fetch_all(
            connection,
            "SELECT status, count(*) AS n FROM pipeline_runs GROUP BY status",
        )
        runs = CounterMetricFamily(
            f"{PREFIX}_pipeline_runs",
            "Platform runs by final status (blocked means the gate stopped it)",
            labels=["status"],
        )
        for row in rows:
            runs.add_metric([row["status"]], float(row["n"]))
        yield runs

        last = fetch_all(
            connection,
            """
            SELECT extract(epoch from max(started_at)) AS ts
            FROM pipeline_runs
            """,
        )
        timestamp = GaugeMetricFamily(
            f"{PREFIX}_last_run_timestamp_seconds",
            "Unix time of the most recent platform run",
        )
        if last and last[0]["ts"] is not None:
            timestamp.add_metric([], float(last[0]["ts"]))
        yield timestamp
