"""The raw layer must accept what the fault generator throws at it."""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.integration]

RAW_TABLES = ("customers", "products", "orders", "order_items", "payments")


def test_the_raw_schema_has_the_five_tables(source_db):
    from src.common.db import fetch_all

    rows = fetch_all(
        source_db,
        "SELECT table_name FROM information_schema.tables WHERE table_schema='raw'",
    )
    assert {row["table_name"] for row in rows} == set(RAW_TABLES)


def test_no_business_column_carries_a_constraint(source_db):
    from src.common.db import fetch_all

    # The whole design: a corrupt row must be INGESTABLE so the quality layer
    # can detect it. A constraint here would turn a detectable incident into
    # an un-ingestable row.
    rows = fetch_all(
        source_db,
        """
        SELECT rel.relname AS table_name, con.contype::text AS kind, con.conname AS name
        FROM pg_constraint con
        JOIN pg_class rel ON rel.oid = con.conrelid
        JOIN pg_namespace ns ON ns.oid = rel.relnamespace
        WHERE ns.nspname = 'raw'
        """,
    )
    # Only a primary key on the surrogate ingestion_id, plus NOT NULLs on the
    # three ingestion-metadata columns, are permitted.
    for row in rows:
        if row["kind"] == "p":
            assert row["name"].endswith("_pkey")
            continue
        assert row["kind"] == "n", f"unexpected {row['kind']} constraint {row['name']}"
        assert any(
            marker in row["name"]
            for marker in ("ingestion_id", "_batch_id", "_ingested_at", "_source")
        ), f"NOT NULL on a business column: {row['name']}"

    assert not [r for r in rows if r["kind"] == "u"], "no UNIQUE constraints allowed"
    assert not [r for r in rows if r["kind"] == "f"], "no FOREIGN KEYs allowed"
    assert not [r for r in rows if r["kind"] == "c"], "no CHECK constraints allowed"


def test_a_corrupt_row_is_accepted_and_a_duplicate_is_preserved(source_db):
    from src.common.db import execute, fetch_value

    # Everything the nine fault families need to land, in one transaction
    # that is rolled back so the test leaves no trace.
    source_db.execute("SAVEPOINT probe")
    try:
        execute(
            source_db,
            "INSERT INTO raw.customers (customer_id, email, state, _batch_id) "
            "VALUES (NULL, 'not-an-email', 'ZZZZ', 'pytest')",
        )
        execute(
            source_db,
            "INSERT INTO raw.products (product_id, price, cost, _batch_id) "
            "VALUES ('p', -99.99, -5, 'pytest')",
        )
        for _ in range(2):
            execute(
                source_db,
                "INSERT INTO raw.orders (order_id, customer_id, order_status, "
                "total_amount, _batch_id) VALUES ('dup', 'nobody', 'teleported', -1, 'pytest')",
            )

        assert fetch_value(
            source_db, "SELECT count(*) FROM raw.customers WHERE _batch_id='pytest'"
        ) == 1
        assert fetch_value(
            source_db, "SELECT count(*) FROM raw.products WHERE price < 0 AND _batch_id='pytest'"
        ) == 1
        # The duplicate survives as a duplicate, under two distinct surrogate
        # keys - which is what makes duplication observable at all.
        assert fetch_value(
            source_db, "SELECT count(*) FROM raw.orders WHERE order_id='dup'"
        ) == 2
        assert fetch_value(
            source_db,
            "SELECT count(DISTINCT ingestion_id) FROM raw.orders WHERE order_id='dup'",
        ) == 2
    finally:
        source_db.execute("ROLLBACK TO SAVEPOINT probe")

    assert fetch_value(
        source_db, "SELECT count(*) FROM raw.orders WHERE order_id='dup'"
    ) == 0


def test_the_dbt_schemas_exist(source_db):
    from src.common.db import fetch_all

    rows = fetch_all(
        source_db,
        "SELECT schema_name FROM information_schema.schemata "
        "WHERE schema_name IN ('raw','staging','analytics','dbt_test_failures')",
    )
    assert len(rows) == 4
