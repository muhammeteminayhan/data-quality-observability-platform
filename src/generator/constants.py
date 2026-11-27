"""Fixed domains and volume targets for the synthetic source data.

Kept in one module because three different places have to agree on them: the
generator that produces the data, the contracts that assert its shape, and the
fault injector that violates it on purpose.
"""

from __future__ import annotations

# Default row counts (section 4 of the specification).
DEFAULT_VOLUMES: dict[str, int] = {
    "customers": 10_000,
    "products": 1_000,
    "orders": 50_000,
    "order_items": 100_000,
    "payments": 50_000,
}

# Postgres datasets, in dependency order: a child is always generated after
# the parent it references, so the clean dataset has no orphans by accident.
POSTGRES_DATASETS: tuple[str, ...] = (
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
)

# Parquet datasets in MinIO.
PARQUET_DATASETS: tuple[str, ...] = (
    "daily_sales",
    "inventory_snapshots",
    "customer_activity",
)

# --- categorical domains ---------------------------------------------------
# These are the accepted-value sets the contracts assert, so an "invalid
# value" fault only has to pick something outside them.

ORDER_STATUSES: tuple[str, ...] = (
    "pending",
    "approved",
    "processing",
    "shipped",
    "delivered",
    "cancelled",
    "returned",
)

# Statuses that imply the order was fulfilled, used by the consistency rules.
FULFILLED_STATUSES: tuple[str, ...] = ("shipped", "delivered")

PAYMENT_TYPES: tuple[str, ...] = (
    "credit_card",
    "debit_card",
    "bank_transfer",
    "voucher",
    "wallet",
    # A refund is a real payment type, not a corruption. Refund rows carry a
    # NEGATIVE payment_value, which is why the financial consistency rule
    # sums only non-refund rows when comparing against the order total - and
    # why `payment_value > 0` is deliberately NOT one of the validity rules.
    "refund",
)

# Payment types that settle an order, i.e. everything except a refund.
SETTLING_PAYMENT_TYPES: tuple[str, ...] = tuple(
    kind for kind in PAYMENT_TYPES if kind != "refund"
)

CUSTOMER_SEGMENTS: tuple[str, ...] = ("consumer", "small_business", "enterprise")

PRODUCT_CATEGORIES: dict[str, tuple[str, ...]] = {
    "electronics": ("phones", "laptops", "audio", "cameras"),
    "home": ("kitchen", "furniture", "bedding", "garden"),
    "fashion": ("menswear", "womenswear", "footwear", "accessories"),
    "sports": ("fitness", "outdoor", "cycling", "team_sports"),
    "books": ("fiction", "non_fiction", "children", "technical"),
}

# Two-letter state codes the contracts accept.
STATES: tuple[str, ...] = (
    "CA", "NY", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "MI",
    "NJ", "VA", "WA", "AZ", "MA", "TN", "IN", "MO", "MD", "WI",
)

COUNTRY = "US"

# --- business rules the clean data obeys -----------------------------------

# Fraction of orders that carry a discount, and the maximum discount rate.
DISCOUNT_RATE = 0.18
MAX_DISCOUNT_FRACTION = 0.25

# Fraction of delivered orders that are later refunded. A refund legitimately
# makes payments diverge from the order total, which is why the financial
# consistency rules allow for it instead of demanding equality.
REFUND_RATE = 0.04

# Orders paid in more than one instalment. A single order can therefore have
# several payment rows that only sum to the total.
MULTI_PAYMENT_RATE = 0.12

# Money is rounded to cents everywhere. The consistency rules allow a
# tolerance of this many currency units per order so that summing rounded
# line items does not look like a data-quality failure.
ROUNDING_TOLERANCE = 0.02

# Weekday weighting for order volume. Index 0 is Monday. Weekends are
# genuinely quieter, which the volume baseline has to understand: comparing a
# Sunday against a Wednesday would flag normal behaviour as an anomaly.
WEEKDAY_VOLUME_WEIGHTS: tuple[float, ...] = (
    1.00,  # Monday
    1.05,  # Tuesday
    1.02,  # Wednesday
    1.04,  # Thursday
    0.98,  # Friday
    0.62,  # Saturday
    0.55,  # Sunday
)

# Price bounds per category, in whole currency units.
CATEGORY_PRICE_RANGE: dict[str, tuple[float, float]] = {
    "electronics": (49.0, 2400.0),
    "home": (12.0, 900.0),
    "fashion": (9.0, 350.0),
    "sports": (15.0, 1200.0),
    "books": (5.0, 90.0),
}

# Gross margin applied to derive cost from price.
MARGIN_RANGE: tuple[float, float] = (0.18, 0.55)

# Items per order.
ITEMS_PER_ORDER_RANGE: tuple[int, int] = (1, 5)
QUANTITY_RANGE: tuple[int, int] = (1, 4)
