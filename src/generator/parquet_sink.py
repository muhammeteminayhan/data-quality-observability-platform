"""Build the three Parquet datasets and store them in MinIO.

Layout:

    s3://<bucket>/daily_sales/dt=YYYY-MM-DD/part-0.parquet
    s3://<bucket>/inventory_snapshots/dt=YYYY-MM-DD/part-0.parquet
    s3://<bucket>/customer_activity/dt=YYYY-MM-DD/part-0.parquet

Each dataset carries BOTH an event timestamp (``event_ts``) and an ingestion
timestamp (``ingested_at``). The freshness detector prefers the event
timestamp and falls back to the ingestion timestamp, recording which one it
used - a dataset that is written on time but contains week-old events is
stale in a way an object mtime cannot show.

The datasets are derived from the same generated batch that goes into
Postgres, so lineage from the raw tables to the lake is real rather than
asserted: daily_sales genuinely aggregates the order items that were written
to raw.order_items.
"""

from __future__ import annotations

import random
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any

import pandas as pd

from src.common.logging import get_logger
from src.common.storage import object_key, s3_uri, write_parquet
from src.generator.records import GeneratedData

LOGGER = get_logger(__name__)


def _ingested_at(partition_date: Any) -> datetime:
    """Ingestion timestamp for a partition.

    Derived from the partition date rather than from ``now()`` so a rebuild
    with the same seed produces identical bytes. The freshness scenarios move
    this value explicitly instead of relying on wall-clock drift.
    """
    stamp = datetime.combine(
        partition_date, datetime.min.time(), tzinfo=UTC
    ).replace(hour=23, minute=30)
    # Same trap as the Postgres generator: for TODAY's partition, 23:30 is in
    # the future, which makes the freshness lag clamp to 0 and the dataset
    # read as perfectly fresh no matter what. Clamping to the current instant
    # keeps a same-day partition honest while leaving every past partition
    # exactly as it was (the ceiling never binds there), so a rebuild stays
    # byte-identical.
    now = datetime.now(UTC)
    return min(stamp, now)


def build_daily_sales(data: GeneratedData) -> pd.DataFrame:
    """Sales aggregated to (date, category, subcategory).

    Built from the order items that were actually generated, so the totals
    here and the totals in raw.order_items agree by construction.
    """
    by_product: dict[str, dict[str, Any]] = {
        row["product_id"]: row for row in data.products
    }
    buckets: dict[tuple[str, str], dict[str, Any]] = {}
    orders_seen: dict[tuple[str, str], set[str]] = defaultdict(set)

    for item in data.order_items:
        product = by_product.get(item["product_id"])
        if product is None:
            # An orphan item cannot be attributed to a category. It is counted
            # separately rather than dropped, so an injected
            # referential-integrity fault shows up in the lake as well.
            key = ("unknown", "unknown")
        else:
            key = (product["category"], product["subcategory"])

        bucket = buckets.setdefault(
            key,
            {
                "event_date": data.partition_date,
                "category": key[0],
                "subcategory": key[1],
                "units_sold": 0,
                "gross_amount": 0.0,
                "discount_amount": 0.0,
                "net_amount": 0.0,
                "order_count": 0,
            },
        )
        quantity = int(item["quantity"] or 0)
        unit_price = float(item["unit_price"] or 0.0)
        discount = float(item["discount"] or 0.0)
        bucket["units_sold"] += quantity
        bucket["gross_amount"] += unit_price * quantity
        bucket["discount_amount"] += discount
        bucket["net_amount"] += float(item["line_total"] or 0.0)
        orders_seen[key].add(item["order_id"])

    rows: list[dict[str, Any]] = []
    for key in sorted(buckets):
        bucket = buckets[key]
        bucket["order_count"] = len(orders_seen[key])
        bucket["gross_amount"] = round(bucket["gross_amount"], 2)
        bucket["discount_amount"] = round(bucket["discount_amount"], 2)
        bucket["net_amount"] = round(bucket["net_amount"], 2)
        bucket["avg_order_value"] = round(
            bucket["net_amount"] / bucket["order_count"], 2
        ) if bucket["order_count"] else 0.0
        bucket["event_ts"] = _ingested_at(data.partition_date)
        bucket["ingested_at"] = _ingested_at(data.partition_date)
        bucket["batch_id"] = data.batch_id
        rows.append(bucket)

    return pd.DataFrame(rows)


def build_inventory_snapshots(data: GeneratedData) -> pd.DataFrame:
    """One stock position per product for the partition date."""
    rng = random.Random(data.seed + 101)
    sold_by_product: dict[str, int] = defaultdict(int)
    for item in data.order_items:
        sold_by_product[item["product_id"]] += int(item["quantity"] or 0)

    rows: list[dict[str, Any]] = []
    for product in data.products:
        sold = sold_by_product.get(product["product_id"], 0)
        on_hand = max(0, rng.randint(20, 900) - sold)
        reserved = min(on_hand, rng.randint(0, 40))
        reorder_point = rng.randint(10, 120)
        rows.append(
            {
                "snapshot_date": data.partition_date,
                "product_id": product["product_id"],
                "category": product["category"],
                "stock_on_hand": on_hand,
                "stock_reserved": reserved,
                "stock_available": on_hand - reserved,
                "reorder_point": reorder_point,
                "needs_reorder": (on_hand - reserved) < reorder_point,
                "units_sold_today": sold,
                "unit_cost": float(product["cost"]),
                "stock_value": round(on_hand * float(product["cost"]), 2),
                "event_ts": _ingested_at(data.partition_date),
                "ingested_at": _ingested_at(data.partition_date),
                "batch_id": data.batch_id,
            }
        )
    return pd.DataFrame(rows)


def build_customer_activity(data: GeneratedData) -> pd.DataFrame:
    """Per-customer engagement for the partition date.

    Only customers that were active are present, so this dataset is naturally
    smaller than raw.customers - a difference the volume baselines have to
    learn rather than treat as loss.
    """
    rng = random.Random(data.seed + 202)
    ordered: dict[str, int] = defaultdict(int)
    revenue: dict[str, float] = defaultdict(float)
    for order in data.orders:
        ordered[order["customer_id"]] += 1
        revenue[order["customer_id"]] += float(order["total_amount"] or 0.0)

    rows: list[dict[str, Any]] = []
    for customer in data.customers:
        customer_id = customer["customer_id"]
        orders_today = ordered.get(customer_id, 0)
        # An inactive customer appears only sometimes; browsing without buying
        # is normal, buying without a session is not.
        if orders_today == 0 and rng.random() > 0.35:
            continue
        sessions = max(1, orders_today + rng.randint(0, 3))
        page_views = sessions * rng.randint(2, 18)
        rows.append(
            {
                "activity_date": data.partition_date,
                "customer_id": customer_id,
                "segment": customer["segment"],
                "state": customer["state"],
                "sessions": sessions,
                "page_views": page_views,
                "cart_additions": rng.randint(0, max(1, page_views // 4)),
                "orders_placed": orders_today,
                "revenue": round(revenue.get(customer_id, 0.0), 2),
                "conversion_rate": round(orders_today / sessions, 4),
                "event_ts": _ingested_at(data.partition_date),
                "ingested_at": _ingested_at(data.partition_date),
                "batch_id": data.batch_id,
            }
        )
    return pd.DataFrame(rows)


BUILDERS = {
    "daily_sales": build_daily_sales,
    "inventory_snapshots": build_inventory_snapshots,
    "customer_activity": build_customer_activity,
}


def build_all(data: GeneratedData) -> dict[str, pd.DataFrame]:
    return {name: builder(data) for name, builder in BUILDERS.items()}


def write(data: GeneratedData) -> dict[str, dict[str, Any]]:
    """Build and store all three Parquet datasets."""
    partition = data.partition_date.isoformat()
    result: dict[str, dict[str, Any]] = {}

    for name, frame in build_all(data).items():
        key = object_key(name, partition)
        if frame.empty:
            # Writing an empty Parquet file would make a failed build look
            # like a successful one with zero rows. The volume detector must
            # see an absent object instead.
            LOGGER.warning("dataset %s produced no rows; nothing written", name)
            result[name] = {"rows": 0, "key": None, "uri": None}
            continue
        write_parquet(key, frame)
        result[name] = {
            "rows": int(len(frame)),
            "columns": list(frame.columns),
            "key": key,
            "uri": s3_uri(key),
        }
        LOGGER.info("%s <- %d rows", s3_uri(key), len(frame))

    return result


def write_local(data: GeneratedData, directory: str) -> dict[str, str]:
    """Write the Parquet datasets to a local directory instead of MinIO.

    Backs the CLI's ``--output local`` mode, which makes the generator usable
    without the object store running.
    """
    from pathlib import Path

    base = Path(directory)
    written: dict[str, str] = {}
    for name, frame in build_all(data).items():
        if frame.empty:
            continue
        target = base / name / f"dt={data.partition_date.isoformat()}"
        target.mkdir(parents=True, exist_ok=True)
        path = target / "part-0.parquet"
        frame.to_parquet(path, engine="pyarrow", compression="snappy", index=False)
        written[name] = str(path)
        LOGGER.info("%s <- %d rows", path, len(frame))
    return written
