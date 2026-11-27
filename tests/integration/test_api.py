"""The read API, exercised over the network."""

from __future__ import annotations

import httpx
import pytest

pytestmark = [pytest.mark.integration]


def _get(url: str, **kwargs):
    try:
        return httpx.get(url, timeout=20, **kwargs)
    except httpx.HTTPError as exc:
        pytest.skip(f"{url} is not reachable: {exc}")


def test_health_probes_its_dependencies(api_base_url):
    response = _get(f"{api_base_url}/health")
    assert response.status_code == 200
    payload = response.json()
    # A health endpoint that returns ok without touching anything is worse
    # than none at all, so each dependency is reported individually.
    assert set(payload["checks"]) == {"observability_db", "source_db", "object_store"}
    assert payload["checks"]["observability_db"] is True
    assert payload["checks"]["source_db"] is True


def test_summary_separates_unmeasured_from_healthy(api_base_url):
    payload = _get(f"{api_base_url}/summary").json()
    assert payload["datasets_monitored"] == 8
    # Listed apart on purpose: a dataset nobody has measured is not healthy.
    assert "datasets_never_measured" in payload
    assert "datasets_unhealthy_or_degraded" in payload
    assert payload["latest_gate_decision"]["decision"] in (
        "PASS",
        "PASS_WITH_WARNINGS",
        "FAIL",
    )


def test_dataset_detail_returns_every_detector(api_base_url):
    payload = _get(f"{api_base_url}/datasets/raw.orders").json()
    assert payload["dataset"]["dataset_name"] == "raw.orders"
    assert payload["latest_score"]["overall_score"] is not None
    # Freshness must always say which clock it used.
    assert payload["latest_freshness"]["timestamp_source"] in (
        "event_time",
        "ingestion_time",
        "object_mtime",
        "unavailable",
    )
    assert payload["latest_volume"]["baseline_method"] in (
        "rolling_median_mad",
        "absolute_bounds",
        "insufficient_history",
    )
    assert payload["latest_schema"]["column_count"] > 0


def test_unknown_dataset_is_a_404_that_says_what_to_do(api_base_url):
    response = _get(f"{api_base_url}/datasets/does.not.exist")
    assert response.status_code == 404
    assert "contract" in response.json()["detail"]


def test_contract_versions_expose_the_checksum(api_base_url):
    payload = _get(f"{api_base_url}/contracts/raw.orders").json()
    assert payload["count"] >= 1
    assert len(payload["versions"][0]["content_sha256"]) == 64


def test_score_history_comes_from_postgres_not_prometheus(api_base_url):
    payload = _get(f"{api_base_url}/datasets/raw.orders/scores?limit=5").json()
    assert payload["count"] >= 1
    first = payload["scores"][0]
    # Each score carries the run that produced it - a join Prometheus cannot
    # do, which is why business history lives in the database.
    assert first["run_id"]
    assert first["health_status"] in ("healthy", "degraded", "unhealthy", "unknown")


def test_runs_list_and_detail_agree(api_base_url):
    runs = _get(f"{api_base_url}/runs?limit=5").json()
    assert runs["count"] >= 1
    run_id = runs["runs"][0]["run_id"]
    detail = _get(f"{api_base_url}/runs/{run_id}").json()
    assert detail["run"]["run_id"] == run_id
    assert isinstance(detail["validations"], list)


def test_incidents_can_be_filtered(api_base_url):
    everything = _get(f"{api_base_url}/incidents?limit=200").json()
    resolved = _get(f"{api_base_url}/incidents?status=RESOLVED&limit=200").json()
    assert resolved["count"] <= everything["count"]
    assert all(item["status"] == "RESOLVED" for item in resolved["incidents"])


def test_the_api_cannot_resolve_an_incident(api_base_url):
    # Resolution requires a verified healthy re-measurement. If an HTTP call
    # could close an incident, that guarantee would be decorative. Acknowledge
    # is the single permitted write.
    paths = _get(f"{api_base_url}/openapi.json").json()["paths"]
    writable = [path for path, methods in paths.items() if set(methods) - {"get"}]
    assert writable == ["/incidents/{incident_id}/acknowledge"], writable
