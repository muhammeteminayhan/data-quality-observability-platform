"""Reproducible fault injection across all nine fault families.

Every scenario is registered, named, and runnable on its own:

    python -m src.generator.fault_injector --scenario null_customer_id
    python -m src.generator.fault_injector --profile volume_drop
    python -m src.generator.fault_injector --list

Reproducibility
---------------
A fault that lands on different rows each time cannot be used to prove a
detector works. So:

* rows are selected with an explicit ``ORDER BY ingestion_id`` and a LIMIT -
  never with ``ORDER BY random()``;
* any value that needs to vary comes from a ``random.Random(seed)`` derived
  from the scenario name, so two runs of the same scenario with the same seed
  touch the same rows and write the same values;
* every scenario reports exactly what it changed, and that report is what the
  end-to-end tests assert against.

What a scenario does NOT do
---------------------------
It does not decide whether the fault was detected, and it does not touch the
observability database. The whole point is that detection is the platform's
job: if the injector also recorded the expectation, a broken detector would
still look like it worked.

Each scenario declares ``expected_detector`` and ``expected_severity`` so the
tests can assert that the RIGHT mechanism caught it at the RIGHT severity,
rather than merely that something somewhere failed.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any

import psycopg
from psycopg import sql

from src.common.logging import get_logger
from src.common.severity import Severity
from src.common.storage import (
    object_key,
    read_parquet,
    write_parquet,
)

LOGGER = get_logger(__name__)


# ---------------------------------------------------------------------------
# Result / scenario types
# ---------------------------------------------------------------------------


@dataclass
class FaultResult:
    """What a scenario actually changed."""

    scenario: str
    family: str
    target: str
    rows_affected: int
    detail: dict[str, Any] = field(default_factory=dict)
    expected_detector: str = ""
    expected_severity: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "scenario": self.scenario,
            "family": self.family,
            "target": self.target,
            "rows_affected": self.rows_affected,
            "expected_detector": self.expected_detector,
            "expected_severity": self.expected_severity,
            "detail": self.detail,
        }


@dataclass(frozen=True)
class Scenario:
    """One injectable fault."""

    name: str
    family: str
    target: str
    description: str
    expected_detector: str
    expected_severity: Severity
    apply: Callable[[InjectionContext], FaultResult]
    reversible: bool = True


@dataclass
class InjectionContext:
    """Everything a scenario needs to do its work."""

    connection: psycopg.Connection | None
    partition_date: date
    seed: int
    magnitude: float = 1.0

    def rng(self, scenario: str) -> random.Random:
        """A generator seeded from the base seed and the scenario name.

        Two different scenarios therefore never consume the same stream, and
        the same scenario is repeatable.
        """
        return random.Random(f"{self.seed}:{scenario}")

    @property
    def db(self) -> psycopg.Connection:
        if self.connection is None:
            raise RuntimeError("this scenario needs a database connection")
        return self.connection


_REGISTRY: dict[str, Scenario] = {}


def register(scenario: Scenario) -> Scenario:
    if scenario.name in _REGISTRY:
        raise ValueError(f"duplicate scenario name {scenario.name!r}")
    _REGISTRY[scenario.name] = scenario
    return scenario


def all_scenarios() -> list[Scenario]:
    return [_REGISTRY[name] for name in sorted(_REGISTRY)]


def get_scenario(name: str) -> Scenario:
    try:
        return _REGISTRY[name]
    except KeyError as exc:
        known = ", ".join(sorted(_REGISTRY))
        raise KeyError(f"unknown scenario {name!r}; known scenarios: {known}") from exc


def scenarios_in_family(family: str) -> list[Scenario]:
    return [s for s in all_scenarios() if s.family == family]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _raw(table: str) -> sql.Composed:
    return sql.SQL("{}.{}").format(sql.Identifier("raw"), sql.Identifier(table))


def _update_oldest(
    context: InjectionContext,
    table: str,
    assignments: sql.Composed,
    limit: int,
    where: sql.Composed | None = None,
) -> int:
    """Apply an UPDATE to the N lowest ingestion_ids matching ``where``.

    Deterministic selection by design - ``ORDER BY ingestion_id`` makes the
    affected rows a function of the data, not of the planner's mood.
    """
    predicate = where if where is not None else sql.SQL("TRUE")
    statement = sql.SQL(
        """
        UPDATE {table} AS t
        SET {assignments}
        WHERE t.ingestion_id IN (
            SELECT ingestion_id FROM {table}
            WHERE {predicate}
            ORDER BY ingestion_id
            LIMIT {limit}
        )
        """
    ).format(
        table=_raw(table),
        assignments=assignments,
        predicate=predicate,
        limit=sql.Literal(limit),
    )
    with context.db.cursor() as cursor:
        cursor.execute(statement)
        return cursor.rowcount


def _count(context: InjectionContext, table: str, where: sql.Composed | None = None) -> int:
    predicate = where if where is not None else sql.SQL("TRUE")
    statement = sql.SQL("SELECT count(*) FROM {table} WHERE {predicate}").format(
        table=_raw(table), predicate=predicate
    )
    with context.db.cursor() as cursor:
        cursor.execute(statement)
        row = cursor.fetchone()
    return int(next(iter(row.values()))) if row else 0


# ===========================================================================
# Family 1: missing values
# ===========================================================================


def _null_customer_id(context: InjectionContext) -> FaultResult:
    rows = _update_oldest(
        context,
        "customers",
        sql.SQL("customer_id = NULL"),
        limit=int(40 * context.magnitude),
        where=sql.SQL("customer_id IS NOT NULL"),
    )
    return FaultResult(
        scenario="null_customer_id",
        family="missing_values",
        target="raw.customers",
        rows_affected=rows,
        detail={"column": "customer_id", "set_to": None},
    )


def _null_order_total(context: InjectionContext) -> FaultResult:
    rows = _update_oldest(
        context,
        "orders",
        sql.SQL("total_amount = NULL"),
        limit=int(60 * context.magnitude),
        where=sql.SQL("total_amount IS NOT NULL"),
    )
    return FaultResult(
        scenario="null_order_total",
        family="missing_values",
        target="raw.orders",
        rows_affected=rows,
        detail={"column": "total_amount", "set_to": None},
    )


def _null_payment_value(context: InjectionContext) -> FaultResult:
    rows = _update_oldest(
        context,
        "payments",
        sql.SQL("payment_value = NULL"),
        limit=int(50 * context.magnitude),
        where=sql.SQL("payment_value IS NOT NULL"),
    )
    return FaultResult(
        scenario="null_payment_value",
        family="missing_values",
        target="raw.payments",
        rows_affected=rows,
        detail={"column": "payment_value", "set_to": None},
    )


# ===========================================================================
# Family 2: duplicates
# ===========================================================================


def _duplicate_orders(context: InjectionContext) -> FaultResult:
    """Re-insert existing order rows verbatim.

    The raw layer has no unique constraint on order_id, so the copies land and
    each gets its own ingestion_id. That is exactly the shape of a real
    double-delivery from an upstream queue.
    """
    limit = int(120 * context.magnitude)
    statement = sql.SQL(
        """
        INSERT INTO {table} (
            order_id, customer_id, order_status, order_date, approved_at,
            delivered_at, shipping_city, shipping_state, total_amount,
            freight_amount, discount_amount, _batch_id, _source
        )
        SELECT
            order_id, customer_id, order_status, order_date, approved_at,
            delivered_at, shipping_city, shipping_state, total_amount,
            freight_amount, discount_amount, _batch_id, 'fault:duplicate_orders'
        FROM {table}
        ORDER BY ingestion_id
        LIMIT {limit}
        """
    ).format(table=_raw("orders"), limit=sql.Literal(limit))
    with context.db.cursor() as cursor:
        cursor.execute(statement)
        rows = cursor.rowcount
    return FaultResult(
        scenario="duplicate_orders",
        family="duplicates",
        target="raw.orders",
        rows_affected=rows,
        detail={"duplicated_key": "order_id", "copies": 1},
    )


def _duplicate_payments(context: InjectionContext) -> FaultResult:
    limit = int(80 * context.magnitude)
    statement = sql.SQL(
        """
        INSERT INTO {table} (
            payment_id, order_id, payment_type, payment_sequence,
            payment_installments, payment_value, payment_date, _batch_id, _source
        )
        SELECT
            payment_id, order_id, payment_type, payment_sequence,
            payment_installments, payment_value, payment_date, _batch_id,
            'fault:duplicate_payments'
        FROM {table}
        WHERE payment_type <> 'refund'
        ORDER BY ingestion_id
        LIMIT {limit}
        """
    ).format(table=_raw("payments"), limit=sql.Literal(limit))
    with context.db.cursor() as cursor:
        cursor.execute(statement)
        rows = cursor.rowcount
    return FaultResult(
        scenario="duplicate_payments",
        family="duplicates",
        target="raw.payments",
        rows_affected=rows,
        detail={"duplicated_key": "payment_id", "note": "also breaks order payment totals"},
    )


# ===========================================================================
# Family 3: invalid values
# ===========================================================================


def _negative_product_price(context: InjectionContext) -> FaultResult:
    rows = _update_oldest(
        context,
        "products",
        sql.SQL("price = -1 * abs(price), cost = -1 * abs(cost)"),
        limit=int(25 * context.magnitude),
        where=sql.SQL("price > 0"),
    )
    return FaultResult(
        scenario="negative_product_price",
        family="invalid_values",
        target="raw.products",
        rows_affected=rows,
        detail={"columns": ["price", "cost"], "made": "negative"},
    )


def _invalid_order_status(context: InjectionContext) -> FaultResult:
    rng = context.rng("invalid_order_status")
    bogus = rng.choice(["TELEPORTED", "unknown", "", "shipped "])
    rows = _update_oldest(
        context,
        "orders",
        sql.SQL("order_status = {}").format(sql.Literal(bogus)),
        limit=int(45 * context.magnitude),
    )
    return FaultResult(
        scenario="invalid_order_status",
        family="invalid_values",
        target="raw.orders",
        rows_affected=rows,
        detail={"column": "order_status", "set_to": bogus},
    )


def _invalid_email(context: InjectionContext) -> FaultResult:
    rows = _update_oldest(
        context,
        "customers",
        sql.SQL("email = replace(email, '@', '_at_')"),
        limit=int(70 * context.magnitude),
        where=sql.SQL("email LIKE '%@%'"),
    )
    return FaultResult(
        scenario="invalid_email",
        family="invalid_values",
        target="raw.customers",
        rows_affected=rows,
        detail={"column": "email", "broken_by": "removing the @"},
    )


def _zero_quantity(context: InjectionContext) -> FaultResult:
    rows = _update_oldest(
        context,
        "order_items",
        sql.SQL("quantity = 0"),
        limit=int(55 * context.magnitude),
        where=sql.SQL("quantity > 0"),
    )
    return FaultResult(
        scenario="zero_quantity",
        family="invalid_values",
        target="raw.order_items",
        rows_affected=rows,
        detail={"column": "quantity", "set_to": 0},
    )


def _future_order_date(context: InjectionContext) -> FaultResult:
    future = datetime.now(UTC) + timedelta(days=400)
    rows = _update_oldest(
        context,
        "orders",
        sql.SQL("order_date = {}").format(sql.Literal(future)),
        limit=int(30 * context.magnitude),
    )
    return FaultResult(
        scenario="future_order_date",
        family="invalid_values",
        target="raw.orders",
        rows_affected=rows,
        detail={"column": "order_date", "set_to": future.isoformat()},
    )


# ===========================================================================
# Family 4: referential integrity
# ===========================================================================


def _orphan_order_items(context: InjectionContext) -> FaultResult:
    rows = _update_oldest(
        context,
        "order_items",
        sql.SQL("order_id = 'ord_9999999' || ingestion_id::text"),
        limit=int(65 * context.magnitude),
    )
    return FaultResult(
        scenario="orphan_order_items",
        family="referential_integrity",
        target="raw.order_items",
        rows_affected=rows,
        detail={"column": "order_id", "points_to": "non-existent orders"},
    )


def _orphan_orders(context: InjectionContext) -> FaultResult:
    rows = _update_oldest(
        context,
        "orders",
        sql.SQL("customer_id = 'cust_999999' || ingestion_id::text"),
        limit=int(40 * context.magnitude),
    )
    return FaultResult(
        scenario="orphan_orders",
        family="referential_integrity",
        target="raw.orders",
        rows_affected=rows,
        detail={"column": "customer_id", "points_to": "non-existent customers"},
    )


# ===========================================================================
# Family 5: financial inconsistency
# ===========================================================================


def _payment_total_mismatch(context: InjectionContext) -> FaultResult:
    """Change order totals without touching their payments.

    This is the fault the consistency rule must catch and that the refund and
    rounding allowances must NOT mask: the mismatch is a large multiplier, far
    outside the rounding tolerance, and it is applied to orders chosen without
    regard to whether they were refunded.
    """
    rows = _update_oldest(
        context,
        "orders",
        sql.SQL("total_amount = round(total_amount * 3.7, 2)"),
        limit=int(50 * context.magnitude),
        where=sql.SQL("total_amount > 0 AND order_status <> 'cancelled'"),
    )
    return FaultResult(
        scenario="payment_total_mismatch",
        family="financial_inconsistency",
        target="raw.orders",
        rows_affected=rows,
        detail={"column": "total_amount", "multiplied_by": 3.7},
    )


def _line_total_mismatch(context: InjectionContext) -> FaultResult:
    """Break line_total = quantity * unit_price - discount."""
    rows = _update_oldest(
        context,
        "order_items",
        sql.SQL("line_total = round(line_total * 0.4, 2)"),
        limit=int(70 * context.magnitude),
        where=sql.SQL("line_total > 0"),
    )
    return FaultResult(
        scenario="line_total_mismatch",
        family="financial_inconsistency",
        target="raw.order_items",
        rows_affected=rows,
        detail={"column": "line_total", "multiplied_by": 0.4},
    )


# ===========================================================================
# Family 6: volume anomalies
# ===========================================================================


def _volume_drop(context: InjectionContext) -> FaultResult:
    """Remove most of the newest partition's orders.

    Deletes by ingestion_id from the top, so the rows removed are a
    deterministic function of the table's contents.
    """
    total = _count(context, "orders")
    keep = max(1, int(total * 0.25))
    statement = sql.SQL(
        """
        DELETE FROM {table}
        WHERE ingestion_id IN (
            SELECT ingestion_id FROM {table} ORDER BY ingestion_id DESC LIMIT {limit}
        )
        """
    ).format(table=_raw("orders"), limit=sql.Literal(total - keep))
    with context.db.cursor() as cursor:
        cursor.execute(statement)
        rows = cursor.rowcount
    return FaultResult(
        scenario="volume_drop",
        family="volume_anomaly",
        target="raw.orders",
        rows_affected=rows,
        detail={"before": total, "after": total - rows, "retained_fraction": 0.25},
    )


def _volume_spike(context: InjectionContext) -> FaultResult:
    """Multiply the order rows, simulating a replayed upstream batch."""
    total = _count(context, "orders")
    statement = sql.SQL(
        """
        INSERT INTO {table} (
            order_id, customer_id, order_status, order_date, approved_at,
            delivered_at, shipping_city, shipping_state, total_amount,
            freight_amount, discount_amount, _batch_id, _source
        )
        SELECT
            'spike_' || ingestion_id::text, customer_id, order_status, order_date,
            approved_at, delivered_at, shipping_city, shipping_state, total_amount,
            freight_amount, discount_amount, _batch_id, 'fault:volume_spike'
        FROM {table}
        ORDER BY ingestion_id
        LIMIT {limit}
        """
    ).format(table=_raw("orders"), limit=sql.Literal(int(total * 2.5)))
    with context.db.cursor() as cursor:
        cursor.execute(statement)
        rows = cursor.rowcount
    return FaultResult(
        scenario="volume_spike",
        family="volume_anomaly",
        target="raw.orders",
        rows_affected=rows,
        detail={"before": total, "added": rows},
    )


def _parquet_volume_drop(context: InjectionContext) -> FaultResult:
    """Truncate a Parquet partition to a tenth of its rows."""
    dataset = "daily_sales"
    partition = context.partition_date.isoformat()
    key = object_key(dataset, partition)
    frame = read_parquet(key)
    before = len(frame)
    keep = max(1, before // 10)
    write_parquet(key, frame.head(keep))
    return FaultResult(
        scenario="parquet_volume_drop",
        family="volume_anomaly",
        target=f"{dataset}/dt={partition}",
        rows_affected=before - keep,
        detail={"before": before, "after": keep},
    )


# ===========================================================================
# Family 7: freshness anomalies
# ===========================================================================


def _stale_ingestion(context: InjectionContext) -> FaultResult:
    """Push every ingestion timestamp of a table back in time.

    Freshness is measured from the dataset's own timestamps, so moving them is
    what actually makes the dataset stale. Simply not loading would be a
    volume fault instead.
    """
    days = int(5 * context.magnitude)
    # BOTH clocks move. raw.orders declares order_date as its event
    # timestamp, and the detector PREFERS the event time - so moving only
    # `_ingested_at` left the dataset reading perfectly fresh and the
    # scenario proved nothing. A stalled upstream feed means no new events
    # and no new loads, which is what this now reproduces.
    statement = sql.SQL(
        "UPDATE {table} SET _ingested_at = _ingested_at - {interval}::interval, "
        "order_date = order_date - {interval}::interval, "
        "approved_at = approved_at - {interval}::interval"
    ).format(table=_raw("orders"), interval=sql.Literal(f"{days} days"))
    with context.db.cursor() as cursor:
        cursor.execute(statement)
        rows = cursor.rowcount
    return FaultResult(
        scenario="stale_ingestion",
        family="freshness_anomaly",
        target="raw.orders",
        rows_affected=rows,
        detail={
            "columns": ["_ingested_at", "order_date", "approved_at"],
            "moved_back_days": days,
            "note": "both clocks moved; the detector prefers the event time",
        },
    )


def _stale_parquet_events(context: InjectionContext) -> FaultResult:
    """Age the event timestamps inside a Parquet partition.

    The object's own mtime stays current, which is the interesting case: a
    freshness detector that only looked at the object mtime would report this
    dataset as perfectly fresh while every event in it is a week old.
    """
    dataset = "customer_activity"
    partition = context.partition_date.isoformat()
    key = object_key(dataset, partition)
    frame = read_parquet(key)
    days = int(7 * context.magnitude)
    if "event_ts" in frame.columns:
        frame["event_ts"] = frame["event_ts"] - timedelta(days=days)
    write_parquet(key, frame)
    return FaultResult(
        scenario="stale_parquet_events",
        family="freshness_anomaly",
        target=f"{dataset}/dt={partition}",
        rows_affected=int(len(frame)),
        detail={
            "column": "event_ts",
            "moved_back_days": days,
            "object_mtime": "left current on purpose",
        },
    )


# ===========================================================================
# Family 8: schema drift
# ===========================================================================


def _schema_add_column(context: InjectionContext) -> FaultResult:
    with context.db.cursor() as cursor:
        cursor.execute(
            sql.SQL(
                "ALTER TABLE {table} ADD COLUMN IF NOT EXISTS loyalty_tier text"
            ).format(table=_raw("customers"))
        )
    return FaultResult(
        scenario="schema_add_column",
        family="schema_drift",
        target="raw.customers",
        rows_affected=0,
        detail={"added_column": "loyalty_tier", "expected_classification": "ADDITIVE"},
    )


def _schema_drop_column(context: InjectionContext) -> FaultResult:
    with context.db.cursor() as cursor:
        cursor.execute(
            sql.SQL("ALTER TABLE {table} DROP COLUMN IF EXISTS phone").format(
                table=_raw("customers")
            )
        )
    return FaultResult(
        scenario="schema_drop_column",
        family="schema_drift",
        target="raw.customers",
        rows_affected=0,
        detail={
            "dropped_column": "phone",
            "expected_classification": "COLUMN_REMOVAL",
        },
    )


def _schema_type_change(context: InjectionContext) -> FaultResult:
    """Widen a numeric column to text.

    numeric -> text always succeeds, so this is a schema change a real
    pipeline could make by accident, and it silently disables every numeric
    comparison downstream.
    """
    with context.db.cursor() as cursor:
        cursor.execute(
            sql.SQL(
                "ALTER TABLE {table} ALTER COLUMN freight_amount TYPE text "
                "USING freight_amount::text"
            ).format(table=_raw("orders"))
        )
    return FaultResult(
        scenario="schema_type_change",
        family="schema_drift",
        target="raw.orders",
        rows_affected=0,
        detail={
            "column": "freight_amount",
            "from": "numeric",
            "to": "text",
            "expected_classification": "TYPE_CHANGE",
        },
    )


def _schema_rename_column(context: InjectionContext) -> FaultResult:
    """Rename a column.

    From the outside this is indistinguishable from a drop plus an add, which
    is why the detector is only ever allowed to report
    COLUMN_RENAME_CANDIDATE: the two cases produce identical observations and
    nothing in the data can separate them.
    """
    with context.db.cursor() as cursor:
        cursor.execute(
            sql.SQL(
                "ALTER TABLE {table} RENAME COLUMN zip_code TO postal_code"
            ).format(table=_raw("customers"))
        )
    return FaultResult(
        scenario="schema_rename_column",
        family="schema_drift",
        target="raw.customers",
        rows_affected=0,
        detail={
            "from": "zip_code",
            "to": "postal_code",
            "expected_classification": "COLUMN_RENAME_CANDIDATE",
            "note": "a candidate, never a confirmed rename",
        },
    )


# ===========================================================================
# Family 9: distribution drift
# ===========================================================================


def _price_distribution_shift(context: InjectionContext) -> FaultResult:
    """Shift unit prices upward on a large slice of the newest items.

    Applied to a majority of rows so the shift is statistically real at the
    sample sizes involved - a handful of changed rows would be exactly the
    "too small to conclude anything" case the drift module refuses to call.
    """
    total = _count(context, "order_items")
    limit = int(total * 0.6)
    rows = _update_oldest(
        context,
        "order_items",
        sql.SQL(
            "unit_price = round(unit_price * 2.8, 2), "
            "line_total = round(unit_price * 2.8 * quantity - discount, 2)"
        ),
        limit=limit,
        where=sql.SQL("unit_price > 0"),
    )
    return FaultResult(
        scenario="price_distribution_shift",
        family="distribution_drift",
        target="raw.order_items",
        rows_affected=rows,
        detail={"column": "unit_price", "multiplied_by": 2.8, "fraction": 0.6},
    )


def _category_distribution_shift(context: InjectionContext) -> FaultResult:
    """Collapse the category mix towards a single value.

    A categorical shift, which is what the chi-square style test is for.
    """
    total = _count(context, "products")
    rows = _update_oldest(
        context,
        "products",
        sql.SQL("category = 'electronics', subcategory = 'phones'"),
        limit=int(total * 0.7),
    )
    return FaultResult(
        scenario="category_distribution_shift",
        family="distribution_drift",
        target="raw.products",
        rows_affected=rows,
        detail={"column": "category", "collapsed_to": "electronics", "fraction": 0.7},
    )


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------

for _scenario in (
    # 1. missing values
    Scenario("null_customer_id", "missing_values", "raw.customers",
             "NULL out customer_id on a slice of customers",
             "validation", Severity.CRITICAL, _null_customer_id),
    Scenario("null_order_total", "missing_values", "raw.orders",
             "NULL out total_amount on a slice of orders",
             "validation", Severity.CRITICAL, _null_order_total),
    Scenario("null_payment_value", "missing_values", "raw.payments",
             "NULL out payment_value on a slice of payments",
             "validation", Severity.CRITICAL, _null_payment_value),
    # 2. duplicates
    Scenario("duplicate_orders", "duplicates", "raw.orders",
             "Re-insert existing order rows so order_id repeats",
             "validation", Severity.ERROR, _duplicate_orders),
    Scenario("duplicate_payments", "duplicates", "raw.payments",
             "Re-insert existing payment rows so payment_id repeats",
             "validation", Severity.ERROR, _duplicate_payments),
    # 3. invalid values
    Scenario("negative_product_price", "invalid_values", "raw.products",
             "Flip product price and cost negative",
             "validation", Severity.ERROR, _negative_product_price),
    Scenario("invalid_order_status", "invalid_values", "raw.orders",
             "Set order_status to a value outside the accepted set",
             "validation", Severity.ERROR, _invalid_order_status),
    Scenario("invalid_email", "invalid_values", "raw.customers",
             "Break the email format by removing the @",
             "validation", Severity.WARNING, _invalid_email),
    Scenario("zero_quantity", "invalid_values", "raw.order_items",
             "Set order item quantity to zero",
             "validation", Severity.ERROR, _zero_quantity),
    Scenario("future_order_date", "invalid_values", "raw.orders",
             "Move order_date more than a year into the future",
             "validation", Severity.ERROR, _future_order_date),
    # 4. referential integrity
    Scenario("orphan_order_items", "referential_integrity", "raw.order_items",
             "Point order items at order ids that do not exist",
             "validation", Severity.CRITICAL, _orphan_order_items),
    Scenario("orphan_orders", "referential_integrity", "raw.orders",
             "Point orders at customer ids that do not exist",
             "validation", Severity.CRITICAL, _orphan_orders),
    # 5. financial inconsistency
    Scenario("payment_total_mismatch", "financial_inconsistency", "raw.orders",
             "Inflate order totals without touching their payments",
             "validation", Severity.CRITICAL, _payment_total_mismatch),
    Scenario("line_total_mismatch", "financial_inconsistency", "raw.order_items",
             "Break line_total = quantity * unit_price - discount",
             "validation", Severity.ERROR, _line_total_mismatch),
    # 6. volume
    Scenario("volume_drop", "volume_anomaly", "raw.orders",
             "Delete 75 percent of the order rows",
             "volume", Severity.CRITICAL, _volume_drop, reversible=False),
    # CRITICAL, not ERROR: a 2.5x spike gives a measured robust z of 67.45
    # against a critical_z of 6.0, so the detector escalates - correctly. The
    # declared severity is what the scenario EXPECTS the platform to report,
    # so it has to match the magnitude the scenario actually injects.
    Scenario("volume_spike", "volume_anomaly", "raw.orders",
             "Insert 2.5x the order rows, as a replayed upstream batch",
             "volume", Severity.CRITICAL, _volume_spike),
    Scenario("parquet_volume_drop", "volume_anomaly", "daily_sales",
             "Truncate a Parquet partition to a tenth of its rows",
             "volume", Severity.CRITICAL, _parquet_volume_drop, reversible=False),
    # 7. freshness
    Scenario("stale_ingestion", "freshness_anomaly", "raw.orders",
             "Move every _ingested_at back several days",
             "freshness", Severity.CRITICAL, _stale_ingestion),
    Scenario("stale_parquet_events", "freshness_anomaly", "customer_activity",
             "Age the event timestamps while leaving the object mtime current",
             "freshness", Severity.ERROR, _stale_parquet_events),
    # 8. schema drift
    Scenario("schema_add_column", "schema_drift", "raw.customers",
             "Add a new nullable column (additive change)",
             "schema", Severity.INFO, _schema_add_column),
    Scenario("schema_drop_column", "schema_drift", "raw.customers",
             "Drop a column the contract requires",
             "schema", Severity.CRITICAL, _schema_drop_column, reversible=False),
    Scenario("schema_type_change", "schema_drift", "raw.orders",
             "Change a numeric column to text",
             "schema", Severity.CRITICAL, _schema_type_change),
    Scenario("schema_rename_column", "schema_drift", "raw.customers",
             "Rename a column, which can only ever be a rename CANDIDATE",
             "schema", Severity.ERROR, _schema_rename_column),
    # 9. distribution drift
    Scenario("price_distribution_shift", "distribution_drift", "raw.order_items",
             "Shift unit_price upward on most rows",
             "distribution", Severity.WARNING, _price_distribution_shift),
    Scenario("category_distribution_shift", "distribution_drift", "raw.products",
             "Collapse the product category mix towards one value",
             "distribution", Severity.WARNING, _category_distribution_shift),
):
    register(_scenario)


# Named profiles: a profile is just an ordered list of scenarios, so the DAG
# param `fault_profile` can request a single fault or a combination.
PROFILES: dict[str, tuple[str, ...]] = {
    "clean": (),
    "missing_values": tuple(s.name for s in scenarios_in_family("missing_values")),
    "duplicates": tuple(s.name for s in scenarios_in_family("duplicates")),
    "invalid_values": tuple(s.name for s in scenarios_in_family("invalid_values")),
    "referential_integrity": tuple(
        s.name for s in scenarios_in_family("referential_integrity")
    ),
    "financial_inconsistency": tuple(
        s.name for s in scenarios_in_family("financial_inconsistency")
    ),
    "volume_drop": ("volume_drop",),
    "volume_spike": ("volume_spike",),
    "freshness": ("stale_ingestion",),
    "schema_drift": ("schema_add_column", "schema_type_change"),
    "distribution_drift": tuple(
        s.name for s in scenarios_in_family("distribution_drift")
    ),
    # Everything at once, for the "many simultaneous faults" end-to-end case.
    "all_families": (
        "null_customer_id",
        "duplicate_orders",
        "negative_product_price",
        "orphan_order_items",
        "payment_total_mismatch",
        "volume_spike",
        "stale_ingestion",
        "schema_add_column",
        "price_distribution_shift",
    ),
}


def inject(
    scenario_names: list[str],
    *,
    connection: psycopg.Connection | None,
    partition_date: date,
    seed: int,
    magnitude: float = 1.0,
) -> list[FaultResult]:
    """Run the named scenarios in order and return what each one changed."""
    context = InjectionContext(
        connection=connection,
        partition_date=partition_date,
        seed=seed,
        magnitude=magnitude,
    )
    results: list[FaultResult] = []
    for name in scenario_names:
        scenario = get_scenario(name)
        LOGGER.info("injecting %s (%s -> %s)", scenario.name, scenario.family, scenario.target)
        result = scenario.apply(context)
        result.expected_detector = scenario.expected_detector
        result.expected_severity = scenario.expected_severity.value
        LOGGER.info(
            "%s affected %d rows; expect %s to report %s",
            result.scenario,
            result.rows_affected,
            result.expected_detector,
            result.expected_severity,
        )
        results.append(result)
    return results


def resolve_profile(profile: str) -> tuple[str, ...]:
    if profile in PROFILES:
        return PROFILES[profile]
    # A profile name may also be a single scenario name, which keeps the DAG
    # param flexible without a second parameter.
    if profile in _REGISTRY:
        return (profile,)
    known = ", ".join(sorted(PROFILES) + sorted(_REGISTRY))
    raise KeyError(f"unknown fault profile {profile!r}; known: {known}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m src.generator.fault_injector",
        description="Inject a reproducible data-quality fault.",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--scenario", help="a single scenario name")
    group.add_argument("--profile", help="a named profile, or a single scenario name")
    group.add_argument(
        "--list", action="store_true", help="list every scenario and profile"
    )
    parser.add_argument(
        "--date",
        default=None,
        help="partition date the fault applies to (YYYY-MM-DD, default today)",
    )
    parser.add_argument("--seed", type=int, default=None, help="override the seed")
    parser.add_argument(
        "--magnitude",
        type=float,
        default=1.0,
        help="scale how many rows a scenario touches (default 1.0)",
    )
    parser.add_argument(
        "--json", action="store_true", help="print the result as JSON"
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    from src.common.config import get_settings
    from src.common.db import source_connection

    args = _build_parser().parse_args(argv)

    if args.list:
        print(f"{'scenario':<30} {'family':<26} {'target':<20} detector/severity")
        print("-" * 100)
        for scenario in all_scenarios():
            print(
                f"{scenario.name:<30} {scenario.family:<26} {scenario.target:<20} "
                f"{scenario.expected_detector}/{scenario.expected_severity.value}"
            )
        print("\nprofiles:")
        for name, members in sorted(PROFILES.items()):
            print(f"  {name:<26} {', '.join(members) if members else '(none)'}")
        return 0

    settings = get_settings()
    seed = args.seed if args.seed is not None else settings.seed
    partition = (
        date.fromisoformat(args.date)
        if args.date
        else datetime.now(UTC).date()
    )
    names = [args.scenario] if args.scenario else list(resolve_profile(args.profile))

    if not names:
        LOGGER.info("profile %r injects nothing (clean run)", args.profile)
        if args.json:
            print(json.dumps([]))
        return 0

    # Parquet-only scenarios need no database connection, but opening one is
    # harmless and keeps the code path single.
    with source_connection(read_only=False) as connection:
        results = inject(
            names,
            connection=connection,
            partition_date=partition,
            seed=seed,
            magnitude=args.magnitude,
        )

    if args.json:
        print(json.dumps([r.as_dict() for r in results], indent=2, default=str))
    else:
        for result in results:
            print(
                f"{result.scenario}: {result.rows_affected} rows in {result.target} "
                f"(expect {result.expected_detector} -> {result.expected_severity})"
            )
    return 0


if __name__ == "__main__":
    sys.exit(main())
