"""Write generated records into the permissive raw layer of commerce_db.

``COPY`` rather than executemany: 160k rows go in a few seconds instead of
minutes, and psycopg 3's binary copy keeps the Python-side types (datetime,
Decimal, None) intact so a NULL stays a NULL rather than becoming the string
"None" - which would turn a deliberate missing-value fault into an
undetectable text value instead.

The writer never validates. That is the point of the raw layer: if this module
rejected a bad row, the quality platform downstream would have nothing to
find, and a real incident would have been converted into a silent data loss.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import psycopg
from psycopg import sql

from src.common.db import execute, fetch_value, source_connection
from src.common.logging import get_logger
from src.generator.constants import POSTGRES_DATASETS
from src.generator.records import GeneratedData, columns_of

LOGGER = get_logger(__name__)


def _copy_rows(
    connection: psycopg.Connection,
    table: str,
    columns: Sequence[str],
    rows: Sequence[dict[str, Any]],
) -> int:
    """COPY a batch of dict rows into raw.<table>."""
    if not rows:
        return 0

    statement = sql.SQL("COPY {}.{} ({}) FROM STDIN").format(
        sql.Identifier("raw"),
        sql.Identifier(table),
        sql.SQL(", ").join(sql.Identifier(name) for name in columns),
    )

    with connection.cursor() as cursor, cursor.copy(statement) as copy:
        for row in rows:
            copy.write_row(tuple(row.get(name) for name in columns))
    return len(rows)


def write(data: GeneratedData, *, truncate: bool = False) -> dict[str, int]:
    """Write a generated batch into the raw schema.

    ``truncate`` empties the five raw tables first. It is used when building a
    fresh baseline; the normal path appends, because successive partitions are
    supposed to accumulate so the volume baseline has history to work with.
    """
    written: dict[str, int] = {}

    with source_connection(read_only=False) as connection:
        if truncate:
            # One statement so the tables are emptied atomically. These are
            # the platform's own synthetic tables, recreated by the generator
            # on demand, and only reached through the explicit `--truncate`
            # flag or `reset`.
            connection.execute(
                sql.SQL("TRUNCATE {} RESTART IDENTITY").format(
                    sql.SQL(", ").join(
                        sql.SQL("{}.{}").format(sql.Identifier("raw"), sql.Identifier(name))
                        for name in POSTGRES_DATASETS
                    )
                )
            )
            LOGGER.info("truncated raw tables before load")

        for table in POSTGRES_DATASETS:
            rows = data.table(table)
            columns = columns_of(rows)
            count = _copy_rows(connection, table, columns, rows)
            written[table] = count
            LOGGER.info("raw.%s <- %d rows", table, count)

    return written


def row_counts() -> dict[str, int]:
    """Current row count of each raw table."""
    counts: dict[str, int] = {}
    with source_connection() as connection:
        for table in POSTGRES_DATASETS:
            counts[table] = int(
                fetch_value(
                    connection,
                    sql.SQL("SELECT count(*) FROM {}.{}").format(
                        sql.Identifier("raw"), sql.Identifier(table)
                    ),
                )
                or 0
            )
    return counts


def batch_counts(batch_id: str) -> dict[str, int]:
    """Row count per table for one batch id."""
    counts: dict[str, int] = {}
    with source_connection() as connection:
        for table in POSTGRES_DATASETS:
            counts[table] = int(
                fetch_value(
                    connection,
                    sql.SQL("SELECT count(*) FROM {}.{} WHERE _batch_id = %s").format(
                        sql.Identifier("raw"), sql.Identifier(table)
                    ),
                    (batch_id,),
                )
                or 0
            )
    return counts


def delete_batch(batch_id: str) -> dict[str, int]:
    """Remove one batch again.

    Used by the test fixtures so a scenario can undo itself without emptying
    tables another test is relying on.
    """
    removed: dict[str, int] = {}
    with source_connection(read_only=False) as connection:
        for table in POSTGRES_DATASETS:
            removed[table] = execute(
                connection,
                sql.SQL("DELETE FROM {}.{} WHERE _batch_id = %s").format(
                    sql.Identifier("raw"), sql.Identifier(table)
                ),
                (batch_id,),
            )
    return removed
