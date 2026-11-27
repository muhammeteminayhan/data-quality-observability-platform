"""The alert receiver and the metrics exporter."""

from __future__ import annotations

import re
import uuid

import httpx
import pytest

pytestmark = [pytest.mark.integration]


def _get(url: str, **kwargs):
    try:
        return httpx.get(url, timeout=20, **kwargs)
    except httpx.HTTPError as exc:
        pytest.skip(f"{url} is not reachable: {exc}")


def _token() -> str:
    from src.common.config import get_settings

    return get_settings().alerting.webhook_token


# --- alert webhook --------------------------------------------------------


def test_health(webhook_base_url):
    assert _get(f"{webhook_base_url}/health").json()["observability_db"] == "reachable"


def test_an_alert_is_accepted_then_deduplicated(webhook_base_url):
    alert = {
        "status": "firing",
        "dataset": "raw.orders",
        "rule": f"pytest-{uuid.uuid4()}",
        "severity": "critical",
        "title": "pytest alert",
        "message": "integration test",
    }
    headers = {"X-DQ-Token": _token()}

    first = httpx.post(f"{webhook_base_url}/alerts", json=alert, headers=headers, timeout=20)
    assert first.status_code == 202
    assert first.json()["result"] == "received"

    again = httpx.post(f"{webhook_base_url}/alerts", json=alert, headers=headers, timeout=20)
    # 200 rather than a 4xx: the sender did nothing wrong, and a non-2xx
    # would only make it retry harder.
    assert again.status_code == 200
    assert again.json()["result"] == "duplicate"
    assert again.json()["fingerprint"] == first.json()["fingerprint"]


def test_a_different_alert_is_not_treated_as_a_duplicate(webhook_base_url):
    headers = {"X-DQ-Token": _token()}
    base = {"status": "firing", "dataset": "raw.orders", "title": "t"}
    one = httpx.post(
        f"{webhook_base_url}/alerts",
        json=base | {"rule": f"pytest-a-{uuid.uuid4()}"},
        headers=headers,
        timeout=20,
    )
    two = httpx.post(
        f"{webhook_base_url}/alerts",
        json=base | {"rule": f"pytest-b-{uuid.uuid4()}"},
        headers=headers,
        timeout=20,
    )
    assert one.json()["fingerprint"] != two.json()["fingerprint"]
    assert two.json()["result"] == "received"


def test_a_bearer_token_is_accepted(webhook_base_url):
    # Grafana sends credentials as an Authorization header with a scheme, not
    # as an arbitrary custom header.
    response = httpx.post(
        f"{webhook_base_url}/alerts",
        json={"status": "firing", "rule": f"pytest-bearer-{uuid.uuid4()}", "title": "b"},
        headers={"Authorization": f"Bearer {_token()}"},
        timeout=20,
    )
    assert response.status_code == 202


def test_a_wrong_token_is_refused(webhook_base_url):
    response = httpx.post(
        f"{webhook_base_url}/alerts",
        json={"status": "firing"},
        headers={"X-DQ-Token": "nonsense"},
        timeout=20,
    )
    assert response.status_code == 401


def test_a_malformed_body_is_recorded_rather_than_dropped(webhook_base_url):
    response = httpx.post(
        f"{webhook_base_url}/alerts",
        content=b"not json at all",
        headers={"X-DQ-Token": _token(), "Content-Type": "application/json"},
        timeout=20,
    )
    assert response.status_code == 422
    # A sender that has started posting rubbish is itself an incident, so the
    # rejection is stored rather than discarded.
    listed = _get(f"{webhook_base_url}/alerts?limit=200").json()
    assert any(item["status"] == "rejected" for item in listed["deliveries"])


# --- metrics exporter -----------------------------------------------------

REQUIRED_METRICS = (
    "dq_quality_score",
    "dq_dataset_has_critical_failure",
    "dq_dimensions_measured",
    "dq_freshness_lag_seconds",
    "dq_row_count",
    "dq_validation_runs_total",
    "dq_rule_failures_total",
    "dq_schema_changes_total",
    "dq_incidents",
    "dq_gate_decisions_total",
    "dq_dbt_models",
    "dq_dbt_tests",
    "dq_pipeline_runs_total",
    "dq_last_run_timestamp_seconds",
    "dq_collection_failed",
)

FORBIDDEN_LABELS = (
    "run_id",
    "incident_id",
    "customer_id",
    "order_id",
    "payment_id",
    "error",
    "message",
)


def test_every_required_metric_is_published(exporter_url):
    body = _get(f"{exporter_url}/metrics").text
    missing = [name for name in REQUIRED_METRICS if name not in body]
    assert not missing, f"missing metrics: {missing}"


def test_the_exporter_reports_that_it_can_read_the_database(exporter_url):
    body = _get(f"{exporter_url}/metrics").text
    assert "dq_collection_failed 0.0" in body


def test_no_unbounded_label_is_ever_published(exporter_url):
    body = _get(f"{exporter_url}/metrics").text
    labels: set[str] = set()
    for match in re.finditer(r"^dq_[a-z_]+\{([^}]*)\}", body, re.MULTILINE):
        for pair in match.group(1).split(","):
            if "=" in pair:
                labels.add(pair.split("=", 1)[0].strip())
    offenders = sorted(labels & set(FORBIDDEN_LABELS))
    assert not offenders, f"unbounded label(s) published: {offenders}"


def test_every_freshness_series_names_the_clock_it_used(exporter_url):
    # A dataset with no usable timestamp publishes NO series - zero would
    # read as perfectly fresh.
    body = _get(f"{exporter_url}/metrics").text
    found = list(re.finditer(r"^dq_freshness_lag_seconds\{([^}]*)\}", body, re.MULTILINE))
    assert found, "no freshness series published at all"
    for match in found:
        assert "timestamp_source=" in match.group(1)
        assert 'timestamp_source="unavailable"' not in match.group(1)


def test_an_inconclusive_drift_test_publishes_no_statistic(exporter_url):
    """A statistic the platform refused to stand behind must not be plotted.

    NOTE on parsing: the prometheus_client exposition format sorts labels
    ALPHABETICALLY, so a series reads
    `{column="x",dataset="y",method="psi"}` regardless of the order the
    labels were declared in. Labels are matched by name here for that reason.
    """
    body = _get(f"{exporter_url}/metrics").text

    def series(metric: str) -> dict[tuple[str, str], float]:
        found: dict[tuple[str, str], float] = {}
        for line in body.splitlines():
            if not line.startswith(metric + "{"):
                continue
            labels_part, _, value = line.rpartition("}")
            labels = dict(
                pair.split("=", 1)
                for pair in labels_part[len(metric) + 1 :].split(",")
                if "=" in pair
            )
            key = (
                labels.get("dataset", "").strip('"'),
                labels.get("column", "").strip('"'),
            )
            found[key] = float(value)
        return found

    inconclusive = series("dq_distribution_inconclusive")
    statistics = series("dq_distribution_statistic")

    assert inconclusive or statistics, "the drift module published nothing at all"

    # Every column that could not be concluded on must carry no statistic.
    for key, flag in inconclusive.items():
        if flag == 1.0:
            assert key not in statistics, f"statistic published for inconclusive {key}"

    # And every published statistic must be flagged as conclusive.
    for key in statistics:
        assert inconclusive.get(key, 0.0) == 0.0, f"{key} is both conclusive and not"
