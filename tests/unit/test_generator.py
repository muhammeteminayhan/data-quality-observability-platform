"""Generator determinism and the internal consistency of clean data."""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from src.generator.cli import scaled_volumes
from src.generator.constants import DEFAULT_VOLUMES, WEEKDAY_VOLUME_WEIGHTS
from src.generator.records import generate

pytestmark = pytest.mark.unit

SMALL = {
    "customers": 200,
    "products": 60,
    "orders": 400,
    "order_items": 800,
    "payments": 400,
}
WHEN = date(2024, 1, 15)
TOLERANCE = 0.02


@pytest.fixture(scope="module")
def batch():
    return generate(seed=4242, partition_date=WHEN, batch_id="b", volumes=SMALL)


def test_the_same_seed_produces_an_identical_dataset():
    a = generate(seed=99, partition_date=WHEN, batch_id="b", volumes=SMALL)
    b = generate(seed=99, partition_date=WHEN, batch_id="b", volumes=SMALL)
    assert a.fingerprint() == b.fingerprint()
    assert a.row_counts() == b.row_counts()


def test_a_different_seed_produces_a_different_dataset():
    a = generate(seed=99, partition_date=WHEN, batch_id="b", volumes=SMALL)
    c = generate(seed=100, partition_date=WHEN, batch_id="b", volumes=SMALL)
    assert a.fingerprint() != c.fingerprint()


def test_requested_volumes_are_honoured(batch):
    counts = batch.row_counts()
    assert counts["customers"] == SMALL["customers"]
    assert counts["products"] == SMALL["products"]
    assert counts["orders"] == SMALL["orders"]
    # Items and payments vary per order by design (1-5 items, instalments,
    # refunds), so they land near the target rather than exactly on it.
    assert counts["order_items"] == pytest.approx(SMALL["order_items"], rel=0.25)


def test_natural_keys_are_unique(batch):
    for table, key in (
        ("customers", "customer_id"),
        ("products", "product_id"),
        ("orders", "order_id"),
        ("order_items", "order_item_id"),
        ("payments", "payment_id"),
    ):
        values = [row[key] for row in batch.table(table)]
        assert len(values) == len(set(values)), f"{table}.{key} has duplicates"


def test_no_business_key_is_null(batch):
    assert all(row["customer_id"] for row in batch.customers)
    assert all(row["order_id"] for row in batch.orders)
    assert all(row["total_amount"] is not None for row in batch.orders)


def test_there_are_no_orphans(batch):
    customers = {row["customer_id"] for row in batch.customers}
    products = {row["product_id"] for row in batch.products}
    orders = {row["order_id"] for row in batch.orders}

    assert all(row["customer_id"] in customers for row in batch.orders)
    assert all(row["order_id"] in orders for row in batch.order_items)
    assert all(row["product_id"] in products for row in batch.order_items)
    assert all(row["order_id"] in orders for row in batch.payments)


def test_the_line_total_identity_holds(batch):
    for item in batch.order_items:
        expected = item["quantity"] * item["unit_price"] - item["discount"]
        assert abs(item["line_total"] - expected) <= TOLERANCE


def test_order_totals_equal_their_items_plus_freight(batch):
    items_by_order: dict[str, float] = {}
    for item in batch.order_items:
        items_by_order[item["order_id"]] = (
            items_by_order.get(item["order_id"], 0.0) + item["line_total"]
        )
    for order in batch.orders:
        items_total = items_by_order.get(order["order_id"])
        if items_total is None:
            continue
        assert abs(order["total_amount"] - (items_total + order["freight_amount"])) <= TOLERANCE


def test_settling_payments_equal_the_order_total(batch):
    settled: dict[str, float] = {}
    for payment in batch.payments:
        if payment["payment_type"] == "refund":
            continue
        settled[payment["order_id"]] = (
            settled.get(payment["order_id"], 0.0) + payment["payment_value"]
        )
    checked = 0
    for order in batch.orders:
        if order["order_status"] == "cancelled":
            continue
        paid = settled.get(order["order_id"])
        if paid is None:
            continue
        assert abs(order["total_amount"] - paid) <= TOLERANCE
        checked += 1
    assert checked > 0, "no orders were actually compared"


def test_clean_data_really_does_contain_refunds(batch):
    # The realistic complication a naive consistency rule would misread as a
    # failure. If the generator stopped producing these, the rule design
    # would no longer be exercised at all.
    refunds = [p for p in batch.payments if p["payment_type"] == "refund"]
    assert refunds, "clean data must contain refunds"
    assert all(p["payment_value"] < 0 for p in refunds)


def test_clean_data_contains_multi_instalment_orders(batch):
    counts: dict[str, int] = {}
    for payment in batch.payments:
        if payment["payment_type"] != "refund":
            counts[payment["order_id"]] = counts.get(payment["order_id"], 0) + 1
    assert any(value > 1 for value in counts.values())


def test_cancelled_orders_are_genuinely_unpaid(batch):
    paid = {p["order_id"] for p in batch.payments}
    cancelled = [o for o in batch.orders if o["order_status"] == "cancelled"]
    assert cancelled, "clean data must contain cancelled orders"
    assert all(o["order_id"] not in paid for o in cancelled)


def test_event_timestamps_are_never_in_the_future(batch):
    now = datetime.now(UTC)
    assert all(order["order_date"] <= now for order in batch.orders)


def test_enum_values_stay_inside_their_domains(batch):
    from src.generator.constants import (
        CUSTOMER_SEGMENTS,
        ORDER_STATUSES,
        PAYMENT_TYPES,
        PRODUCT_CATEGORIES,
        STATES,
    )

    assert {o["order_status"] for o in batch.orders} <= set(ORDER_STATUSES)
    assert {p["payment_type"] for p in batch.payments} <= set(PAYMENT_TYPES)
    assert {c["segment"] for c in batch.customers} <= set(CUSTOMER_SEGMENTS)
    assert {c["state"] for c in batch.customers} <= set(STATES)
    assert {p["category"] for p in batch.products} <= set(PRODUCT_CATEGORIES)


def test_prices_are_positive(batch):
    assert all(p["price"] > 0 for p in batch.products)
    assert all(i["unit_price"] > 0 for i in batch.order_items)
    assert all(i["quantity"] > 0 for i in batch.order_items)


def test_the_weekday_curve_makes_weekends_quieter():
    monday = date(2024, 1, 15)
    sunday = date(2024, 1, 21)
    assert monday.weekday() == 0 and sunday.weekday() == 6

    weekday_volumes = scaled_volumes(10_000, partition_date=monday)
    weekend_volumes = scaled_volumes(10_000, partition_date=sunday)
    assert weekend_volumes["orders"] < weekday_volumes["orders"]
    expected = round(10_000 * WEEKDAY_VOLUME_WEIGHTS[6])
    assert weekend_volumes["orders"] == expected


def test_the_weekday_curve_can_be_switched_off():
    sunday = date(2024, 1, 21)
    exact = scaled_volumes(10_000, partition_date=sunday, apply_weekday=False)
    assert exact["orders"] == 10_000


def test_scaling_keeps_the_table_ratios():
    half = scaled_volumes(DEFAULT_VOLUMES["orders"] // 2, partition_date=date(2024, 1, 15))
    assert half["customers"] == DEFAULT_VOLUMES["customers"] // 2
    assert half["products"] == DEFAULT_VOLUMES["products"] // 2
