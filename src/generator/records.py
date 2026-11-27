"""Deterministic synthetic source data.

Determinism is a hard requirement: the same ``--seed`` must produce a
byte-identical dataset, because the volume baselines, the distribution
reference profiles and the end-to-end tests all compare one run against
another. Anything that varies between runs would show up as drift that the
platform then has to explain.

How determinism is achieved
---------------------------
* one ``random.Random(seed)`` instance, threaded through every builder;
* ``Faker`` seeded per instance via ``seed_instance`` rather than the global
  ``Faker.seed``, so a concurrently running Faker elsewhere in the process
  cannot perturb it;
* every timestamp derives from ``partition_date``, never from ``now()``;
* ids are positional (``cust_000001``), never random or hash based;
* no iteration over a set or an unordered dict anywhere in the output path.

Clean data is internally consistent by construction, not by luck: order
totals are computed from the line items that were actually generated, and
payments are derived from the order total. That matters because the
consistency rules are supposed to pass on clean data and fail only when the
fault injector breaks the relationship on purpose.
"""

from __future__ import annotations

import hashlib
import json
import random
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from typing import Any

from faker import Faker

from src.generator.constants import (
    CATEGORY_PRICE_RANGE,
    COUNTRY,
    CUSTOMER_SEGMENTS,
    DISCOUNT_RATE,
    FULFILLED_STATUSES,
    ITEMS_PER_ORDER_RANGE,
    MARGIN_RANGE,
    MAX_DISCOUNT_FRACTION,
    MULTI_PAYMENT_RATE,
    ORDER_STATUSES,
    PRODUCT_CATEGORIES,
    QUANTITY_RANGE,
    REFUND_RATE,
    SETTLING_PAYMENT_TYPES,
    STATES,
)

# Category names in a fixed order. Iterating the dict directly would be
# deterministic in CPython 3.7+, but an explicit tuple makes it independent of
# that guarantee and of any future edit that reorders the mapping.
_CATEGORY_NAMES: tuple[str, ...] = tuple(sorted(PRODUCT_CATEGORIES))


def _money(value: float) -> float:
    """Round to cents.

    Every monetary value passes through here so that the rounding tolerance in
    the consistency rules has a single, documented origin.
    """
    return round(value + 1e-9, 2)


@dataclass
class GeneratedData:
    """One batch of source records, ready to be written."""

    partition_date: date
    batch_id: str
    seed: int
    customers: list[dict[str, Any]] = field(default_factory=list)
    products: list[dict[str, Any]] = field(default_factory=list)
    orders: list[dict[str, Any]] = field(default_factory=list)
    order_items: list[dict[str, Any]] = field(default_factory=list)
    payments: list[dict[str, Any]] = field(default_factory=list)

    def table(self, name: str) -> list[dict[str, Any]]:
        try:
            return getattr(self, name)  # type: ignore[no-any-return]
        except AttributeError as exc:
            raise KeyError(f"unknown table {name!r}") from exc

    def row_counts(self) -> dict[str, int]:
        return {
            "customers": len(self.customers),
            "products": len(self.products),
            "orders": len(self.orders),
            "order_items": len(self.order_items),
            "payments": len(self.payments),
        }

    def fingerprint(self) -> str:
        """A stable sha256 over the whole batch.

        Used by the determinism test: two runs with the same seed must produce
        the same digest, and two different seeds must not.
        """
        digest = hashlib.sha256()
        for name in ("customers", "products", "orders", "order_items", "payments"):
            digest.update(name.encode())
            for row in self.table(name):
                digest.update(
                    json.dumps(row, sort_keys=True, default=str).encode("utf-8")
                )
        return digest.hexdigest()


class SourceDataGenerator:
    """Builds a consistent batch of commerce records."""

    def __init__(self, seed: int, partition_date: date, batch_id: str) -> None:
        self.seed = seed
        self.partition_date = partition_date
        self.batch_id = batch_id
        # Both generators are derived from the same seed but kept separate so
        # that changing the number of Faker calls does not shift the numeric
        # stream, and vice versa.
        self._rng = random.Random(seed)
        self._faker = Faker("en_US")
        self._faker.seed_instance(seed)
        # Captured once so every row in a batch shares one ceiling; sampling
        # now() per row would make the output depend on how long generation
        # took and break determinism for a same-day partition.
        self._now = datetime.now(UTC)

    # -- timestamps -------------------------------------------------------
    def _timestamp_within_day(self, offset_days: int = 0) -> datetime:
        """A timestamp inside the partition day, derived from the seed.

        Business hours are weighted towards the middle of the day, which gives
        the distribution module something real to profile.
        """
        base = datetime.combine(
            self.partition_date - timedelta(days=offset_days),
            time.min,
            tzinfo=UTC,
        )
        hour = min(23, max(0, int(self._rng.gauss(14, 4))))
        stamp = base + timedelta(
            hours=hour,
            minutes=self._rng.randrange(60),
            seconds=self._rng.randrange(60),
        )
        # A whole day is generated in one go, so when the partition IS today
        # most of those timestamps fall later than the current instant. Real
        # orders cannot come from the future, and leaving them would make the
        # contract rule `order_date_not_in_future` fail on data that is
        # otherwise perfectly clean - 42,769 rows of it, measured.
        #
        # Clamping keeps the generator deterministic for any past partition
        # (where the ceiling never binds) while making a same-day run look
        # like a real intraday load.
        ceiling = self._now
        return min(stamp, ceiling) if stamp > ceiling else stamp

    # -- customers --------------------------------------------------------
    def build_customers(self, count: int) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for index in range(1, count + 1):
            first = self._faker.first_name()
            last = self._faker.last_name()
            # The local part is derived from the positional index so the
            # address is unique and reproducible; a Faker email could repeat.
            email = f"{first.lower()}.{last.lower()}.{index}@example.com"
            rows.append(
                {
                    "customer_id": f"cust_{index:06d}",
                    # A deliberately coarser identity: several customer_ids
                    # can share one customer_unique_id, the way a real CRM has
                    # duplicate accounts for one person. The uniqueness rules
                    # therefore apply to customer_id, not to this column.
                    "customer_unique_id": f"uniq_{((index - 1) // 1 + 1):06d}",
                    "first_name": first,
                    "last_name": last,
                    "email": email,
                    "phone": f"+1555{index:07d}",
                    "city": self._faker.city(),
                    "state": self._rng.choice(STATES),
                    "zip_code": f"{self._rng.randrange(10000, 99999):05d}",
                    "country": COUNTRY,
                    "segment": self._rng.choices(
                        CUSTOMER_SEGMENTS, weights=(0.72, 0.21, 0.07), k=1
                    )[0],
                    # Customers pre-date the orders that reference them.
                    "created_at": self._timestamp_within_day(
                        offset_days=self._rng.randrange(30, 400)
                    ),
                    "_batch_id": self.batch_id,
                }
            )
        return rows

    # -- products ---------------------------------------------------------
    def build_products(self, count: int) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for index in range(1, count + 1):
            category = self._rng.choice(_CATEGORY_NAMES)
            subcategory = self._rng.choice(PRODUCT_CATEGORIES[category])
            low, high = CATEGORY_PRICE_RANGE[category]
            # Log-uniform: real catalogues have many cheap items and a thin
            # tail of expensive ones, which is a far more interesting shape for
            # the distribution-drift module than a flat uniform.
            price = _money(low * (high / low) ** self._rng.random())
            margin = self._rng.uniform(*MARGIN_RANGE)
            rows.append(
                {
                    "product_id": f"prod_{index:05d}",
                    "product_name": f"{subcategory.replace('_', ' ').title()} {index}",
                    "category": category,
                    "subcategory": subcategory,
                    "price": price,
                    "cost": _money(price * (1.0 - margin)),
                    "weight_kg": round(self._rng.uniform(0.05, 25.0), 3),
                    "is_active": self._rng.random() > 0.08,
                    "created_at": self._timestamp_within_day(
                        offset_days=self._rng.randrange(30, 800)
                    ),
                    "_batch_id": self.batch_id,
                }
            )
        return rows

    # -- orders, items and payments --------------------------------------
    def build_orders(
        self,
        count: int,
        customers: list[dict[str, Any]],
        products: list[dict[str, Any]],
        target_items: int | None = None,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
        """Build orders together with their items and payments.

        They are produced in one pass because they are not independent: an
        order's total is the sum of its items, and its payments are derived
        from that total. Generating them separately and reconciling afterwards
        is how inconsistent "clean" data gets created.
        """
        if not customers or not products:
            raise ValueError("orders need at least one customer and one product")

        orders: list[dict[str, Any]] = []
        items: list[dict[str, Any]] = []
        payments: list[dict[str, Any]] = []

        # Average items per order needed to land near the requested total.
        avg_items = (
            max(1.0, target_items / count)
            if target_items and count
            else sum(ITEMS_PER_ORDER_RANGE) / 2
        )

        item_sequence_global = 0
        payment_sequence_global = 0

        for index in range(1, count + 1):
            order_id = f"ord_{index:07d}"
            customer = customers[self._rng.randrange(len(customers))]
            order_dt = self._timestamp_within_day()
            status = self._rng.choices(
                ORDER_STATUSES,
                weights=(0.06, 0.08, 0.10, 0.18, 0.49, 0.06, 0.03),
                k=1,
            )[0]

            # Items
            item_count = max(1, int(round(self._rng.gauss(avg_items, 0.9))))
            item_count = min(item_count, ITEMS_PER_ORDER_RANGE[1])
            line_total_sum = 0.0
            for sequence in range(1, item_count + 1):
                product = products[self._rng.randrange(len(products))]
                quantity = self._rng.randint(*QUANTITY_RANGE)
                unit_price = float(product["price"])
                gross = unit_price * quantity
                discount = (
                    _money(gross * self._rng.uniform(0.02, MAX_DISCOUNT_FRACTION))
                    if self._rng.random() < DISCOUNT_RATE
                    else 0.0
                )
                line_total = _money(gross - discount)
                line_total_sum += line_total
                item_sequence_global += 1
                items.append(
                    {
                        "order_item_id": f"oi_{item_sequence_global:08d}",
                        "order_id": order_id,
                        "product_id": product["product_id"],
                        "seller_id": f"seller_{(item_sequence_global % 250) + 1:04d}",
                        "item_sequence": sequence,
                        "quantity": quantity,
                        "unit_price": _money(unit_price),
                        "discount": discount,
                        "line_total": line_total,
                        "shipping_limit_date": order_dt
                        + timedelta(days=self._rng.randint(2, 14)),
                        "_batch_id": self.batch_id,
                    }
                )

            freight = _money(self._rng.uniform(0.0, 24.0))
            order_discount = _money(
                sum(float(row["discount"]) for row in items[-item_count:])
            )
            total_amount = _money(line_total_sum + freight)

            delivered_at = (
                order_dt + timedelta(days=self._rng.randint(2, 11))
                if status in FULFILLED_STATUSES
                else None
            )
            orders.append(
                {
                    "order_id": order_id,
                    "customer_id": customer["customer_id"],
                    "order_status": status,
                    "order_date": order_dt,
                    "approved_at": (
                        order_dt + timedelta(minutes=self._rng.randint(1, 600))
                        if status != "pending"
                        else None
                    ),
                    "delivered_at": delivered_at,
                    "shipping_city": customer["city"],
                    "shipping_state": customer["state"],
                    "total_amount": total_amount,
                    "freight_amount": freight,
                    "discount_amount": order_discount,
                    "_batch_id": self.batch_id,
                }
            )

            # Payments. Cancelled orders are genuinely unpaid, so they get no
            # payment row at all - which is why the consistency rule has to
            # scope itself to orders that were not cancelled rather than
            # demanding every order has a payment.
            if status == "cancelled":
                continue

            instalments = 2 if self._rng.random() < MULTI_PAYMENT_RATE else 1
            remaining = total_amount
            for part in range(1, instalments + 1):
                payment_sequence_global += 1
                if part == instalments:
                    value = _money(remaining)
                else:
                    value = _money(total_amount / instalments)
                    remaining = _money(remaining - value)
                payments.append(
                    {
                        "payment_id": f"pay_{payment_sequence_global:08d}",
                        "order_id": order_id,
                        "payment_type": self._rng.choice(SETTLING_PAYMENT_TYPES),
                        "payment_sequence": part,
                        "payment_installments": instalments,
                        "payment_value": value,
                        "payment_date": order_dt
                        + timedelta(minutes=self._rng.randint(1, 240) * part),
                        "_batch_id": self.batch_id,
                    }
                )

            # A refund is a NEGATIVE payment row. Clean data contains these on
            # purpose: they are the realistic reason a naive
            # sum(payments) == total_amount check would fail, and the contract
            # rule has to be written to tolerate them.
            if status == "returned" or self._rng.random() < REFUND_RATE:
                payment_sequence_global += 1
                payments.append(
                    {
                        "payment_id": f"pay_{payment_sequence_global:08d}",
                        "order_id": order_id,
                        "payment_type": "refund",
                        "payment_sequence": instalments + 1,
                        "payment_installments": instalments,
                        "payment_value": _money(-total_amount),
                        "payment_date": order_dt
                        + timedelta(days=self._rng.randint(3, 30)),
                        "_batch_id": self.batch_id,
                    }
                )

        return orders, items, payments

    # -- top level --------------------------------------------------------
    def build(self, volumes: dict[str, int]) -> GeneratedData:
        data = GeneratedData(
            partition_date=self.partition_date,
            batch_id=self.batch_id,
            seed=self.seed,
        )
        data.customers = self.build_customers(volumes["customers"])
        data.products = self.build_products(volumes["products"])
        data.orders, data.order_items, data.payments = self.build_orders(
            volumes["orders"],
            data.customers,
            data.products,
            target_items=volumes.get("order_items"),
        )
        return data


def generate(
    *,
    seed: int,
    partition_date: date,
    batch_id: str,
    volumes: dict[str, int],
) -> GeneratedData:
    """Convenience wrapper used by the CLI and the tests."""
    return SourceDataGenerator(seed, partition_date, batch_id).build(volumes)


def columns_of(rows: Iterable[dict[str, Any]]) -> list[str]:
    """Column order of a batch, taken from the first row.

    Every row in a batch is built by the same code path, so the first row's
    keys describe them all; the Postgres writer relies on that to build one
    COPY statement per table.
    """
    for row in rows:
        return list(row.keys())
    return []
