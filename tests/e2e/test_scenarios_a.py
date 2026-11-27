"""End-to-end scenarios 1-5: clean baseline and four fault families."""

from __future__ import annotations

import pytest

from tests.e2e.conftest import inject, observations, observe

pytestmark = [pytest.mark.e2e]


# --- 1 --------------------------------------------------------------------
def test_scenario_01_clean_data_reports_healthy(clean_source, today):
    """A clean load must pass every check and the gate."""
    summary = observe(today)
    seen = observations(summary)

    assert len(seen) == 8
    for observation in seen.values():
        assert observation.failed_rules == [], (
            f"{observation.dataset} failed {observation.failed_rules}"
        )
        assert observation.health == "healthy", observation.dataset
        assert observation.score == 100.0, observation.dataset
        assert observation.freshness_status == "ok", observation.dataset
        assert observation.max_severity == "INFO", observation.dataset

    assert summary["gate"]["decision"] == "PASS"
    assert "gate_failed" not in summary


# --- 2 --------------------------------------------------------------------
def test_scenario_02_missing_values_are_critical_and_block_the_gate(clean_source, today):
    """Family 1: NULLs in business keys -> completeness CRITICAL -> gate FAIL."""
    results = inject(["null_customer_id", "null_order_total", "null_payment_value"], today)
    affected = {r.scenario: r.rows_affected for r in results}
    assert all(count > 0 for count in affected.values()), affected

    summary = observe(today)
    seen = observations(summary)

    # Each fault caught by its own completeness rule, on its own dataset.
    assert "customer_id_not_null" in seen["raw.customers"].failed_rules
    assert "total_amount_not_null" in seen["raw.orders"].failed_rules
    assert "payment_value_not_null" in seen["raw.payments"].failed_rules

    for name in ("raw.customers", "raw.orders", "raw.payments"):
        assert seen[name].max_severity == "CRITICAL", name
        # The rule the score can never break: high score, still unhealthy.
        assert seen[name].health == "unhealthy", name

    assert summary["gate"]["decision"] == "FAIL"
    assert "CRITICAL" in summary["gate"]["reason"]


# --- 3 --------------------------------------------------------------------
def test_scenario_03_duplicates_break_uniqueness(clean_source, today):
    """Family 2: re-inserted rows -> uniqueness fails with the real count."""
    results = inject(["duplicate_orders", "duplicate_payments"], today)
    duplicated = {r.scenario: r.rows_affected for r in results}
    assert duplicated["duplicate_orders"] > 0

    summary = observe(today)
    seen = observations(summary)

    assert "order_id_unique" in seen["raw.orders"].failed_rules
    assert "payment_id_unique" in seen["raw.payments"].failed_rules

    # The count is the number of rows participating in a duplicate, which is
    # the original plus the copy - twice what was inserted.
    from src.common.db import fetch_all, observability_connection

    with observability_connection() as connection:
        rows = fetch_all(
            connection,
            """
            SELECT vr.unexpected_count
            FROM validation_results vr JOIN datasets d USING (dataset_id)
            WHERE d.dataset_name = 'raw.orders' AND vr.rule_name = 'order_id_unique'
              AND vr.success = false
            ORDER BY vr.evaluated_at DESC LIMIT 1
            """,
        )
    assert rows and rows[0]["unexpected_count"] == duplicated["duplicate_orders"] * 2


# --- 4 --------------------------------------------------------------------
def test_scenario_04_invalid_values_break_validity(clean_source, today):
    """Family 3: out-of-domain values -> validity rules fail, exact counts."""
    results = inject(
        [
            "negative_product_price",
            "invalid_order_status",
            "zero_quantity",
            "invalid_email",
            "future_order_date",
        ],
        today,
    )
    counts = {r.scenario: r.rows_affected for r in results}

    summary = observe(today)
    seen = observations(summary)

    assert "price_is_positive" in seen["raw.products"].failed_rules
    assert "order_status_in_accepted_set" in seen["raw.orders"].failed_rules
    assert "quantity_is_positive" in seen["raw.order_items"].failed_rules
    assert "email_looks_like_an_address" in seen["raw.customers"].failed_rules
    assert "order_date_not_in_future" in seen["raw.orders"].failed_rules

    from src.common.db import fetch_one, observability_connection

    with observability_connection() as connection:
        row = fetch_one(
            connection,
            """
            SELECT vr.unexpected_count
            FROM validation_results vr JOIN datasets d USING (dataset_id)
            WHERE d.dataset_name = 'raw.products' AND vr.rule_name = 'price_is_positive'
              AND vr.success = false
            ORDER BY vr.evaluated_at DESC LIMIT 1
            """,
        )
    # The detector counted exactly what the injector changed.
    assert row["unexpected_count"] == counts["negative_product_price"]


# --- 5 --------------------------------------------------------------------
def test_scenario_05_referential_integrity_breaks_are_caught(clean_source, today):
    """Family 4: orphan rows -> the cross-table SQL rules fail."""
    inject(["orphan_order_items", "orphan_orders"], today)

    summary = observe(today)
    seen = observations(summary)

    # These span tables, so only the SQL-backed rules can state them.
    assert "every_item_has_a_known_order" in seen["raw.order_items"].failed_rules
    assert "every_order_has_a_known_customer" in seen["raw.orders"].failed_rules
    assert seen["raw.order_items"].max_severity == "CRITICAL"
    assert seen["raw.orders"].max_severity == "CRITICAL"
    assert summary["gate"]["decision"] == "FAIL"
