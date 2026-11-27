"""Shared fixtures.

The unit tests must run with no services at all, so nothing here connects to
anything at import time. The integration and e2e fixtures are the ones that
touch the stack, and they skip with a clear reason when it is not up - a
skipped test that says why is useful; one that fails with a connection error
tells you nothing about the code.
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, date, datetime

import pytest


def _stack_is_up() -> bool:
    try:
        from src.common.config import get_settings
        from src.common.db import ping

        settings = get_settings()
        return ping(settings.observability) and ping(settings.source)
    except Exception:
        return False


@pytest.fixture(scope="session")
def stack() -> None:
    """Skip the whole test if the platform is not running."""
    if not _stack_is_up():
        pytest.skip(
            "the platform stack is not reachable; start it with "
            "./scripts/start.sh before running integration or e2e tests"
        )


@pytest.fixture(scope="session")
def contracts():
    """Every contract, loaded from the real files."""
    from src.common.config import get_settings
    from src.contracts.loader import load_contracts

    return load_contracts(str(get_settings().contracts_dir))


@pytest.fixture(scope="session")
def contracts_by_name(contracts):
    return {contract.dataset: contract for contract in contracts}


@pytest.fixture
def partition_date() -> date:
    """Today, which is the partition normal operation writes."""
    return datetime.now(UTC).date()


@pytest.fixture
def test_run(stack):
    """A real pipeline_runs row, closed when the test finishes.

    Marked `trigger='test'` so test runs can be told apart from airflow and
    cli runs in every dashboard and query.
    """
    from src.common.run_context import RunContext, ensure_run, finish_run

    context = RunContext(
        run_id=uuid.uuid4(),
        trigger="test",
        logical_date=datetime.now(UTC).date(),
        notes="pytest",
    )
    ensure_run(context)
    yield context
    finish_run(context.run_id, "success", notes="pytest finished")


@pytest.fixture
def source_db(stack):
    """A writable connection to commerce_db."""
    from src.common.db import source_connection

    with source_connection(read_only=False) as connection:
        yield connection


@pytest.fixture
def observability_db(stack):
    from src.common.db import observability_connection

    with observability_connection() as connection:
        yield connection


@pytest.fixture
def api_base_url() -> str:
    return os.environ.get("DQ_API_URL", "http://dq-api:8000")


@pytest.fixture
def webhook_base_url() -> str:
    return os.environ.get("DQ_WEBHOOK_URL", "http://alert-webhook:8000")


@pytest.fixture
def exporter_url() -> str:
    return os.environ.get("DQ_EXPORTER_URL", "http://dq-exporter:9091")
