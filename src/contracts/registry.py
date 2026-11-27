"""Persist contracts and their versions into observability_db.

The registry is what makes the platform self-describing: ``datasets`` and
``contract_versions`` are populated purely from the files in ``contracts/``,
so the dashboards and the API never hold a hardcoded dataset list.

Versioning rule
---------------
A row is keyed on (dataset, version, checksum). That means:

* editing a contract and bumping the version  -> a new row, clearly a change;
* editing a contract and FORGETTING to bump   -> still a new row, because the
  checksum differs. The change cannot pass unnoticed, and the two rows share a
  version string, which is itself the signal that someone skipped the bump.
"""

from __future__ import annotations

import json
from typing import Any

import psycopg

from src.common.db import execute, fetch_all, fetch_one, insert_returning_id
from src.common.logging import get_logger
from src.contracts.models import Contract

LOGGER = get_logger(__name__)


def _physical_location(contract: Contract) -> str:
    return contract.source.location


def upsert_dataset(connection: psycopg.Connection, contract: Contract) -> int:
    """Register (or refresh) the dataset row and return its id."""
    dataset_id = insert_returning_id(
        connection,
        """
        INSERT INTO datasets (
            dataset_name, source_type, layer, physical_location,
            owner, criticality, description
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (dataset_name) DO UPDATE SET
            source_type       = EXCLUDED.source_type,
            layer             = EXCLUDED.layer,
            physical_location = EXCLUDED.physical_location,
            owner             = EXCLUDED.owner,
            criticality       = EXCLUDED.criticality,
            description       = EXCLUDED.description,
            updated_at        = now()
        RETURNING dataset_id
        """,
        (
            contract.dataset,
            contract.source.type,
            contract.layer,
            _physical_location(contract),
            contract.owner,
            contract.criticality,
            contract.description,
        ),
    )
    return int(dataset_id)


def upsert_contract_version(
    connection: psycopg.Connection, contract: Contract, dataset_id: int
) -> int:
    """Record this exact contract content and return its version id."""
    existing = fetch_one(
        connection,
        """
        SELECT contract_version_id
        FROM contract_versions
        WHERE dataset_id = %s AND version = %s AND content_sha256 = %s
        """,
        (dataset_id, contract.version, contract.content_sha256),
    )

    if existing:
        contract_version_id = int(existing["contract_version_id"])
        execute(
            connection,
            """
            UPDATE contract_versions
            SET last_seen_at = now(), is_active = true
            WHERE contract_version_id = %s
            """,
            (contract_version_id,),
        )
    else:
        # Any previous content for this dataset stops being the active one.
        # History is kept; only the "which one is live" flag moves.
        execute(
            connection,
            "UPDATE contract_versions SET is_active = false WHERE dataset_id = %s",
            (dataset_id,),
        )
        contract_version_id = int(
            insert_returning_id(
                connection,
                """
                INSERT INTO contract_versions (
                    dataset_id, version, content_sha256, content, is_active
                )
                VALUES (%s, %s, %s, %s, true)
                RETURNING contract_version_id
                """,
                (
                    dataset_id,
                    contract.version,
                    contract.content_sha256,
                    json.dumps(contract.raw, sort_keys=True, default=str),
                ),
            )
        )
        LOGGER.info(
            "registered contract %s v%s (%s)",
            contract.dataset,
            contract.version,
            contract.content_sha256[:12],
        )

    return contract_version_id


def register(connection: psycopg.Connection, contract: Contract) -> tuple[int, int]:
    """Register one contract. Returns (dataset_id, contract_version_id)."""
    dataset_id = upsert_dataset(connection, contract)
    contract_version_id = upsert_contract_version(connection, contract, dataset_id)
    return dataset_id, contract_version_id


def register_all(
    connection: psycopg.Connection, contracts: list[Contract]
) -> dict[str, tuple[int, int]]:
    """Register every contract and return a name -> (dataset_id, version_id) map."""
    registered: dict[str, tuple[int, int]] = {}
    for contract in contracts:
        registered[contract.dataset] = register(connection, contract)

    # Datasets that no longer have a contract file are deactivated rather than
    # deleted: their historical measurements must stay queryable, but they
    # should disappear from the "currently monitored" views.
    names = [contract.dataset for contract in contracts]
    deactivated = execute(
        connection,
        """
        UPDATE datasets
        SET is_active = false, updated_at = now()
        WHERE is_active = true AND NOT (dataset_name = ANY(%s))
        """,
        (names,),
    )
    if deactivated:
        LOGGER.info("deactivated %d dataset(s) with no contract file", deactivated)

    return registered


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------


def dataset_id_for(connection: psycopg.Connection, dataset_name: str) -> int:
    row = fetch_one(
        connection,
        "SELECT dataset_id FROM datasets WHERE dataset_name = %s",
        (dataset_name,),
    )
    if row is None:
        raise KeyError(
            f"dataset {dataset_name!r} is not registered; "
            "run the contract registration step first"
        )
    return int(row["dataset_id"])


def dataset_ids(connection: psycopg.Connection) -> dict[str, int]:
    return {
        row["dataset_name"]: int(row["dataset_id"])
        for row in fetch_all(connection, "SELECT dataset_name, dataset_id FROM datasets")
    }


def active_contract_version_id(
    connection: psycopg.Connection, dataset_id: int
) -> int | None:
    row = fetch_one(
        connection,
        """
        SELECT contract_version_id
        FROM contract_versions
        WHERE dataset_id = %s AND is_active = true
        ORDER BY last_seen_at DESC
        LIMIT 1
        """,
        (dataset_id,),
    )
    return int(row["contract_version_id"]) if row else None


def contract_history(connection: psycopg.Connection, dataset_name: str) -> list[dict[str, Any]]:
    return fetch_all(
        connection,
        """
        SELECT cv.version, cv.content_sha256, cv.is_active,
               cv.first_seen_at, cv.last_seen_at
        FROM contract_versions cv
        JOIN datasets d ON d.dataset_id = cv.dataset_id
        WHERE d.dataset_name = %s
        ORDER BY cv.first_seen_at DESC
        """,
        (dataset_name,),
    )
