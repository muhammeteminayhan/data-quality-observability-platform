-- ===========================================================================
-- commerce_db : raw ingestion layer
-- ===========================================================================
-- This layer is DELIBERATELY PERMISSIVE. The fault generator has to be able to
-- land every one of the nine fault families here, so the raw tables carry:
--
--   * no NOT NULL on any business column        -> missing-value faults land
--   * no UNIQUE on any natural key              -> duplicate rows are kept
--   * no FOREIGN KEY between raw tables         -> orphan rows are kept
--   * no CHECK constraints                      -> negative prices, zero
--                                                  quantities, unknown enum
--                                                  values and out-of-range
--                                                  dates all land
--
-- The only key is `ingestion_id`, a surrogate that belongs to the ingestion
-- process rather than to the business data. Two byte-identical source rows get
-- two different ingestion_ids, so a duplicate is preserved as a duplicate
-- instead of being silently collapsed or rejected.
--
-- Quality is asserted by the contracts (contracts/*.yml), by Great
-- Expectations, and by the dbt tests on the staging and analytics layers -
-- never by the raw DDL. A constraint here would turn a detectable data-quality
-- incident into an un-ingestable row, which is precisely the failure mode this
-- platform exists to make visible.
--
-- Business columns keep real types (numeric, timestamptz) because that is what
-- a real warehouse looks like; permissiveness comes from the absence of
-- constraints, not from storing everything as text.
-- ===========================================================================

CREATE SCHEMA IF NOT EXISTS raw;

COMMENT ON SCHEMA raw IS
    'Permissive landing zone. No business constraints by design: corrupt rows '
    'must be ingestable so the quality layer can detect them.';

-- ---------------------------------------------------------------------------
-- raw.customers
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.customers (
    ingestion_id        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id         text,
    customer_unique_id  text,
    first_name          text,
    last_name           text,
    email               text,
    phone               text,
    city                text,
    state               text,
    zip_code            text,
    country             text,
    segment             text,
    created_at          timestamptz,
    -- ingestion metadata (owned by the pipeline, not by the source)
    _ingested_at        timestamptz NOT NULL DEFAULT now(),
    _batch_id           text        NOT NULL,
    _source             text        NOT NULL DEFAULT 'generator'
);

COMMENT ON COLUMN raw.customers.ingestion_id IS
    'Surrogate ingestion key. Duplicate source rows keep distinct values here '
    'so duplication stays observable.';

-- ---------------------------------------------------------------------------
-- raw.products
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.products (
    ingestion_id        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id          text,
    product_name        text,
    category            text,
    subcategory         text,
    price               numeric(12, 2),
    cost                numeric(12, 2),
    weight_kg           numeric(10, 3),
    is_active           boolean,
    created_at          timestamptz,
    _ingested_at        timestamptz NOT NULL DEFAULT now(),
    _batch_id           text        NOT NULL,
    _source             text        NOT NULL DEFAULT 'generator'
);

-- ---------------------------------------------------------------------------
-- raw.orders
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.orders (
    ingestion_id        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id            text,
    customer_id         text,
    order_status        text,
    order_date          timestamptz,
    approved_at         timestamptz,
    delivered_at        timestamptz,
    shipping_city       text,
    shipping_state      text,
    total_amount        numeric(14, 2),
    freight_amount      numeric(12, 2),
    discount_amount     numeric(12, 2),
    _ingested_at        timestamptz NOT NULL DEFAULT now(),
    _batch_id           text        NOT NULL,
    _source             text        NOT NULL DEFAULT 'generator'
);

-- ---------------------------------------------------------------------------
-- raw.order_items
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.order_items (
    ingestion_id        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_item_id       text,
    order_id            text,
    product_id          text,
    seller_id           text,
    item_sequence       integer,
    quantity            integer,
    unit_price          numeric(12, 2),
    discount            numeric(12, 2),
    line_total          numeric(14, 2),
    shipping_limit_date timestamptz,
    _ingested_at        timestamptz NOT NULL DEFAULT now(),
    _batch_id           text        NOT NULL,
    _source             text        NOT NULL DEFAULT 'generator'
);

-- ---------------------------------------------------------------------------
-- raw.payments
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.payments (
    ingestion_id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    payment_id           text,
    order_id             text,
    payment_type         text,
    payment_sequence     integer,
    payment_installments integer,
    payment_value        numeric(14, 2),
    payment_date         timestamptz,
    _ingested_at         timestamptz NOT NULL DEFAULT now(),
    _batch_id            text        NOT NULL,
    _source              text        NOT NULL DEFAULT 'generator'
);

-- ---------------------------------------------------------------------------
-- Indexes
-- ---------------------------------------------------------------------------
-- NON-UNIQUE on purpose. These exist so the validation, freshness and volume
-- queries stay fast on 100k+ rows; none of them rejects a row.
CREATE INDEX IF NOT EXISTS ix_customers_customer_id   ON raw.customers   (customer_id);
CREATE INDEX IF NOT EXISTS ix_customers_ingested_at   ON raw.customers   (_ingested_at DESC);
CREATE INDEX IF NOT EXISTS ix_customers_batch         ON raw.customers   (_batch_id);

CREATE INDEX IF NOT EXISTS ix_products_product_id     ON raw.products    (product_id);
CREATE INDEX IF NOT EXISTS ix_products_ingested_at    ON raw.products    (_ingested_at DESC);

CREATE INDEX IF NOT EXISTS ix_orders_order_id         ON raw.orders      (order_id);
CREATE INDEX IF NOT EXISTS ix_orders_customer_id      ON raw.orders      (customer_id);
CREATE INDEX IF NOT EXISTS ix_orders_order_date       ON raw.orders      (order_date);
CREATE INDEX IF NOT EXISTS ix_orders_ingested_at      ON raw.orders      (_ingested_at DESC);
CREATE INDEX IF NOT EXISTS ix_orders_batch            ON raw.orders      (_batch_id);

CREATE INDEX IF NOT EXISTS ix_order_items_order_id    ON raw.order_items (order_id);
CREATE INDEX IF NOT EXISTS ix_order_items_product_id  ON raw.order_items (product_id);
CREATE INDEX IF NOT EXISTS ix_order_items_ingested_at ON raw.order_items (_ingested_at DESC);

CREATE INDEX IF NOT EXISTS ix_payments_order_id       ON raw.payments    (order_id);
CREATE INDEX IF NOT EXISTS ix_payments_date           ON raw.payments    (payment_date);
CREATE INDEX IF NOT EXISTS ix_payments_ingested_at    ON raw.payments    (_ingested_at DESC);
