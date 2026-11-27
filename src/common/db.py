"""PostgreSQL access for the platform, built on psycopg 3.

Two distinct databases are reached from here and they are never mixed:

* ``source_connection()``        -> commerce_db, the warehouse being watched.
                                   Read-only in every code path except the
                                   generator and the fault injector.
* ``observability_connection()`` -> observability_db, the platform's own
                                   metadata store. Written by the platform.

Both are exposed as context managers that commit on success and roll back on
an exception, so a half-written set of observability rows can never be left
behind by a failing check.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from src.common.config import PostgresSettings, get_settings
from src.common.logging import get_logger

LOGGER = get_logger(__name__)

# A statement timeout is set on every connection. A quality check that hangs
# is worse than one that fails: it holds an Airflow worker slot and delays
# every downstream task, and the pipeline has retries for genuine blips.
DEFAULT_STATEMENT_TIMEOUT_MS = 120_000


@contextmanager
def connect(
    settings: PostgresSettings,
    *,
    read_only: bool = False,
    statement_timeout_ms: int = DEFAULT_STATEMENT_TIMEOUT_MS,
    autocommit: bool = False,
) -> Iterator[psycopg.Connection]:
    """Open a connection, commit on success, roll back on failure."""
    connection = psycopg.connect(
        settings.conninfo, row_factory=dict_row, autocommit=autocommit
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                sql.SQL("SET statement_timeout = {}").format(
                    sql.Literal(statement_timeout_ms)
                )
            )
            if read_only:
                # Defence in depth. The checks are meant to observe the
                # warehouse, not change it; if a future rule ever contains a
                # stray UPDATE the server refuses it.
                cursor.execute("SET default_transaction_read_only = on")
        if not autocommit:
            connection.commit()
        yield connection
        if not autocommit:
            connection.commit()
    except Exception:
        if not autocommit:
            connection.rollback()
        raise
    finally:
        connection.close()


@contextmanager
def source_connection(*, read_only: bool = True, **kwargs: Any) -> Iterator[psycopg.Connection]:
    """Connect to commerce_db. Read-only unless explicitly told otherwise."""
    with connect(get_settings().source, read_only=read_only, **kwargs) as conn:
        yield conn


@contextmanager
def observability_connection(**kwargs: Any) -> Iterator[psycopg.Connection]:
    """Connect to observability_db."""
    with connect(get_settings().observability, **kwargs) as conn:
        yield conn


# ---------------------------------------------------------------------------
# Small query helpers
# ---------------------------------------------------------------------------


def fetch_all(
    connection: psycopg.Connection,
    statement: str | sql.Composed,
    params: Sequence[Any] | Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    with connection.cursor() as cursor:
        cursor.execute(statement, params)
        return list(cursor.fetchall())


def fetch_one(
    connection: psycopg.Connection,
    statement: str | sql.Composed,
    params: Sequence[Any] | Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    with connection.cursor() as cursor:
        cursor.execute(statement, params)
        return cursor.fetchone()


def fetch_value(
    connection: psycopg.Connection,
    statement: str | sql.Composed,
    params: Sequence[Any] | Mapping[str, Any] | None = None,
) -> Any:
    """Return the first column of the first row, or None when there is none."""
    row = fetch_one(connection, statement, params)
    if row is None:
        return None
    return next(iter(row.values()))


def execute(
    connection: psycopg.Connection,
    statement: str | sql.Composed,
    params: Sequence[Any] | Mapping[str, Any] | None = None,
) -> int:
    with connection.cursor() as cursor:
        cursor.execute(statement, params)
        return cursor.rowcount


def insert_returning_id(
    connection: psycopg.Connection,
    statement: str | sql.Composed,
    params: Sequence[Any] | Mapping[str, Any] | None = None,
) -> Any:
    """Run an INSERT ... RETURNING <id> and hand back the id."""
    row = fetch_one(connection, statement, params)
    if row is None:
        raise RuntimeError("insert did not return a row; is RETURNING missing?")
    return next(iter(row.values()))


# ---------------------------------------------------------------------------
# Identifier handling
# ---------------------------------------------------------------------------


def qualified_identifier(dotted: str) -> sql.Composed:
    """Turn ``schema.table`` into a safely quoted identifier.

    Dataset names come from contract files, which are configuration rather
    than trusted code. Everything that reaches SQL as an identifier - a table
    or a column name, which cannot be a bound parameter - goes through
    psycopg's Identifier so a contract can never inject SQL.
    """
    parts = [part for part in dotted.split(".") if part]
    if not parts or len(parts) > 2:
        raise ValueError(
            f"expected 'table' or 'schema.table', got {dotted!r}"
        )
    return sql.SQL(".").join(sql.Identifier(part) for part in parts)


def table_exists(connection: psycopg.Connection, dotted: str) -> bool:
    parts = dotted.split(".")
    schema, table = (parts[0], parts[1]) if len(parts) == 2 else ("public", parts[0])
    return bool(
        fetch_value(
            connection,
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
            """,
            (schema, table),
        )
    )


def describe_columns(connection: psycopg.Connection, dotted: str) -> list[dict[str, Any]]:
    """Return ordered column metadata for a table.

    Used by the schema tracker. ``ordinal_position`` ordering matters: the
    fingerprint is taken over the ordered triples, so a reordering is itself
    a change worth seeing.
    """
    parts = dotted.split(".")
    schema, table = (parts[0], parts[1]) if len(parts) == 2 else ("public", parts[0])
    return fetch_all(
        connection,
        """
        SELECT
            column_name,
            data_type,
            is_nullable = 'YES' AS is_nullable,
            ordinal_position
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (schema, table),
    )


def ping(settings: PostgresSettings) -> bool:
    """Return True when the database answers, without raising.

    Used by the health endpoints, where an unreachable database is a reported
    status rather than a crash.
    """
    try:
        with connect(settings, statement_timeout_ms=5_000) as connection:
            return fetch_value(connection, "SELECT 1") == 1
    except Exception as exc:  # noqa: BLE001 - deliberately broad: this is a probe
        LOGGER.warning("database ping failed for %s: %s", settings.redacted(), exc)
        return False
