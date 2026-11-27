"""End-to-end scenarios 9-10: schema drift, distribution drift, recovery."""

from __future__ import annotations

import pytest

from tests.e2e.conftest import (
    _generate,
    incident_events,
    inject,
    observations,
    observe,
    open_incidents,
)

pytestmark = [pytest.mark.e2e]


# --- 9 --------------------------------------------------------------------
def test_scenario_09_schema_drift_is_classified(clean_source, restore_raw_schema, today):
    """Family 8: ADDITIVE / TYPE_CHANGE / COLUMN_REMOVAL / RENAME_CANDIDATE."""
    # A first pass so there is a previous snapshot to diff against.
    observe(today)

    inject(
        [
            "schema_add_column",
            "schema_type_change",
            "schema_rename_column",
            "schema_drop_column",
        ],
        today,
    )

    summary = observe(today)
    seen = observations(summary)

    customers = seen["raw.customers"].schema_changes
    orders = seen["raw.orders"].schema_changes
    assert customers, "no schema change recorded for raw.customers"

    kinds = {c["change_type"] for c in customers}
    assert "ADDITIVE" in kinds
    assert "COLUMN_REMOVAL" in kinds
    assert "COLUMN_RENAME_CANDIDATE" in kinds
    assert "TYPE_CHANGE" in {c["change_type"] for c in orders}

    # A new nullable column breaks nothing, so it stays INFO.
    additive = [c for c in customers if c["change_type"] == "ADDITIVE"]
    assert all(c["severity"] == "INFO" for c in additive)

    # Losing a declared column, or retyping one, is CRITICAL.
    removals = [c for c in customers if c["change_type"] == "COLUMN_REMOVAL"]
    assert any(c["severity"] == "CRITICAL" for c in removals)
    type_changes = [c for c in orders if c["change_type"] == "TYPE_CHANGE"]
    assert any(c["severity"] == "CRITICAL" for c in type_changes)

    # The central claim: a rename and a drop-plus-add are indistinguishable
    # from the outside, so the platform never asserts one.
    candidates = [c for c in customers if c["change_type"] == "COLUMN_RENAME_CANDIDATE"]
    assert candidates
    for candidate in candidates:
        assert candidate["is_confirmed"] is False
        assert 0 < float(candidate["confidence"]) <= 0.85
        assert candidate["previous_column"]
    # One claim per column rather than every possible pairing.
    assert len({c["column_name"] for c in candidates}) == len(candidates)


def test_scenario_09b_a_small_sample_yields_no_drift_claim(clean_source, today):
    """Family 9: the drift module refuses to conclude from too few rows.

    daily_sales holds about 20 rows per partition against a contract minimum
    of 500, so the test is deliberately never run and `inconclusive` is the
    recorded outcome - not a drift finding, and not a silent pass either.
    """
    observe(today)
    summary = observe(today)
    seen = observations(summary)

    drift = seen["daily_sales"].drift
    assert drift, "no drift row recorded for daily_sales at all"
    for row in drift:
        assert row["status"] == "inconclusive"
        assert row["is_significant"] is False
        assert row["statistic"] is None


# --- 10 -------------------------------------------------------------------
def test_scenario_10_recovery_needs_verified_healthy_rechecks(clean_source, today, seed):
    """An incident closes only on positive evidence, never on silence."""
    from src.common.db import fetch_one, observability_connection
    from src.observability.incidents import REQUIRED_HEALTHY_CHECKS

    inject(["null_customer_id"], today)
    broken = observe(today)
    assert broken["gate"]["decision"] == "FAIL"

    live = open_incidents("raw.customers")
    assert live, "the fault should have opened an incident"
    validation_incidents = [i for i in live if i["detector"] == "validation"]
    assert validation_incidents
    incident_id = validation_incidents[0]["incident_id"]

    # The same fault again must NOT create a second incident.
    before = len(live)
    observe(today)
    again = open_incidents("raw.customers")
    assert len(again) == before, "a recurring fault must not multiply incidents"
    recurring = next(i for i in again if i["incident_id"] == incident_id)
    assert recurring["occurrence_count"] >= 2
    # Inside the cooldown the problem is still counted but no longer alerts.
    assert recurring["status"] in ("OPEN", "SUPPRESSED")

    # Now actually fix the data. One healthy pass must NOT be enough.
    _generate(today, seed)
    observe(today)

    events = incident_events(incident_id)
    healthy = [e for e in events if e["event_type"] == "HEALTHY_CHECK"]
    assert healthy, "a passing detector must record positive evidence"
    if len(healthy) < REQUIRED_HEALTHY_CHECKS:
        assert any(
            i["incident_id"] == incident_id for i in open_incidents("raw.customers")
        ), "resolved after a single healthy pass"

    # A further verified healthy measurement closes it.
    for _ in range(REQUIRED_HEALTHY_CHECKS):
        observe(today)

    with observability_connection() as connection:
        row = fetch_one(
            connection,
            "SELECT status, resolved_at, resolution_note FROM incidents WHERE incident_id = %s",
            (incident_id,),
        )
    assert row["status"] == "RESOLVED"
    assert row["resolved_at"] is not None
    # The note records the evidence rather than an assumption.
    assert "verified healthy" in row["resolution_note"]

    final_events = incident_events(incident_id)
    assert any(e["event_type"] == "RESOLVED" for e in final_events)
    # The resolution is preceded by the healthy checks that justified it.
    types = [e["event_type"] for e in final_events]
    assert types.index("RESOLVED") > types.index("HEALTHY_CHECK")


def test_scenario_10b_an_unmeasured_dataset_never_resolves_an_incident(stack):
    """Silence is not evidence.

    A detector that did not run is excluded from the healthy signals, so an
    incident it owns cannot drift towards resolution while nobody is looking.
    """
    from src.observability.anomaly import _detectors_that_ran

    ran = _detectors_that_ran(
        rule_outcomes=[],
        freshness_result=None,
        volume_result=None,
        schema_changes=None,
        drift_results=None,
        score=None,
    )
    assert ran == set(), "nothing ran, so nothing may be treated as healthy"
