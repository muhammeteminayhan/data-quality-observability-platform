-- Analytics: line-item grain fact, enriched with order and product context.
{{ config(materialized='table') }}

select
    i.order_item_id,
    i.order_id,
    i.product_id,
    i.seller_id,
    i.item_sequence,
    o.customer_id,
    o.order_status,
    o.order_date,
    o.order_date::date       as order_day,
    p.category,
    p.subcategory,
    i.quantity,
    i.unit_price,
    i.discount,
    i.line_total,
    i.expected_line_total,
    -- Kept as a column rather than only as a test, so the size of any
    -- discrepancy is queryable instead of merely known to exist.
    round(i.line_total - i.expected_line_total, 2) as line_total_delta,
    p.cost,
    round(i.line_total - (p.cost * i.quantity), 2) as gross_profit,
    i.ingested_at,
    i.batch_id
from {{ ref('stg_order_items') }} i
join {{ ref('stg_orders') }} o   on o.order_id = i.order_id
join {{ ref('stg_products') }} p on p.product_id = i.product_id
