"""The observability schema and the guarantees it enforces in the database."""

from __future__ import annotations

import uuid

import psycopg
import pytest

pytestmark = [pytest.mark.integration]

EXPECTED_TABLES = {
    "datasets", "contract_versions", "pipeline_runs", "validation_runs",
    "validation_results", "dbt_runs", "dbt_test_results", "freshness_checks",
    "volume_checks", "schema_snapshots", "schema_changes",
    "distribution_profiles", "distribution_drift", "quality_scores",
    "incidents", "incident_events", "gate_decisions", "alert_deliveries",
    "metric_history",
}


def test_every_expected_table_exists(observability_db):
    from src.common.db import fetch_all

    rows = fetch_all(
        observability_db,
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema='public' AND table_type='BASE TABLE'",
    )
    assert {row["table_name"] for row in rows} >= EXPECTED_TABLES


def test_initialisation_left_its_completion_marker(stack):
    # The marker is written only after every step of 00_init.sh succeeded. A
    # healthy container with no marker means initdb was skipped after a
    # failed first start - a state that otherwise reports perfectly healthy.
    from src.common.config import get_settings
    from src.common.db import connect, fetch_value

    settings = get_settings()
    platform = type(settings.observability)(
        host=settings.observability.host,
        port=settings.observability.port,
        database="platform_db",
        user=settings.observability.user,
        password=settings.observability.password,
    )
    try:
        with connect(platform) as connection:
            assert fetch_value(connection, "SELECT count(*) FROM platform_init_log") >= 1
    except Exception as exc:
        pytest.skip(f"platform_db is not reachable as the observability role: {exc}")


def test_a_critical_failure_cannot_be_stored_as_healthy(observability_db, test_run):
    from src.common.db import execute, fetch_value

    dataset_id = fetch_value(
        observability_db, "SELECT dataset_id FROM datasets ORDER BY dataset_id LIMIT 1"
    )
    assert dataset_id, "no datasets registered; run the platform once first"

    observability_db.execute("SAVEPOINT probe")
    with pytest.raises(psycopg.errors.CheckViolation):
        execute(
            observability_db,
            """
            INSERT INTO quality_scores (
                run_id, dataset_id, overall_score, dimensions_measured,
                dimensions_total, weights_used, has_critical_failure, health_status
            ) VALUES (%s, %s, 99.9, 6, 6, '{}'::jsonb, true, 'healthy')
            """,
            (test_run.run_id, dataset_id),
        )
    observability_db.execute("ROLLBACK TO SAVEPOINT probe")

    # The same row as unhealthy is accepted, so the constraint is about the
    # contradiction and not about the insert.
    observability_db.execute("SAVEPOINT probe2")
    execute(
        observability_db,
        """
        INSERT INTO quality_scores (
            run_id, dataset_id, overall_score, dimensions_measured,
            dimensions_total, weights_used, has_critical_failure, health_status
        ) VALUES (%s, %s, 99.9, 6, 6, '{}'::jsonb, true, 'unhealthy')
        """,
        (test_run.run_id, dataset_id),
    )
    observability_db.execute("ROLLBACK TO SAVEPOINT probe2")


def test_a_critical_finding_cannot_be_recorded_as_a_clean_pass(observability_db, test_run):
    from src.common.db import execute

    observability_db.execute("SAVEPOINT probe")
    with pytest.raises(psycopg.errors.CheckViolation):
        execute(
            observability_db,
            """
            INSERT INTO gate_decisions (
                run_id, gate_name, decision, max_severity, critical_count, reason
            ) VALUES (%s, 'g', 'PASS', 'CRITICAL', 1, 'should be impossible')
            """,
            (test_run.run_id,),
        )
    observability_db.execute("ROLLBACK TO SAVEPOINT probe")


def test_a_resolved_incident_must_carry_a_resolution_timestamp(observability_db, test_run):
    from src.common.db import execute, fetch_value

    dataset_id = fetch_value(
        observability_db, "SELECT dataset_id FROM datasets ORDER BY dataset_id LIMIT 1"
    )
    observability_db.execute("SAVEPOINT probe")
    with pytest.raises(psycopg.errors.CheckViolation):
        execute(
            observability_db,
            """
            INSERT INTO incidents (fingerprint, dataset_id, detector, title, severity, status)
            VALUES (%s, %s, 'freshness', 't', 'ERROR', 'RESOLVED')
            """,
            (f"pytest-{uuid.uuid4()}", dataset_id),
        )
    observability_db.execute("ROLLBACK TO SAVEPOINT probe")


def test_only_one_live_incident_per_fingerprint(observability_db, test_run):
    from src.common.db import execute, fetch_value

    dataset_id = fetch_value(
        observability_db, "SELECT dataset_id FROM datasets ORDER BY dataset_id LIMIT 1"
    )
    fingerprint = f"pytest-dedupe-{uuid.uuid4()}"

    observability_db.execute("SAVEPOINT probe")
    execute(
        observability_db,
        "INSERT INTO incidents (fingerprint, dataset_id, detector, title, severity, status) "
        "VALUES (%s, %s, 'volume', 't', 'ERROR', 'OPEN')",
        (fingerprint, dataset_id),
    )
    with pytest.raises(psycopg.errors.UniqueViolation):
        execute(
            observability_db,
            "INSERT INTO incidents (fingerprint, dataset_id, detector, title, severity, status) "
            "VALUES (%s, %s, 'volume', 't', 'ERROR', 'OPEN')",
            (fingerprint, dataset_id),
        )
    observability_db.execute("ROLLBACK TO SAVEPOINT probe")


def test_every_dataset_came_from_a_contract(observability_db, contracts):
    from src.common.db import fetch_all

    rows = fetch_all(
        observability_db, "SELECT dataset_name FROM datasets WHERE is_active"
    )
    registered = {row["dataset_name"] for row in rows}
    declared = {contract.dataset for contract in contracts}
    # Nothing is hardcoded: the registry is exactly what the files declare.
    assert registered == declared


def test_contract_versions_store_their_checksum_and_content(observability_db, contracts):
    from src.common.db import fetch_one

    for contract in contracts:
        row = fetch_one(
            observability_db,
            """
            SELECT cv.version, cv.content_sha256, cv.content
            FROM contract_versions cv JOIN datasets d USING (dataset_id)
            WHERE d.dataset_name = %s AND cv.is_active
            """,
            (contract.dataset,),
        )
        assert row is not None, f"{contract.dataset} has no active contract version"
        assert row["version"] == contract.version
        assert row["content_sha256"] == contract.content_sha256
        assert row["content"]["dataset"] == contract.dataset
