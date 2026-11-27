-- Analytics: one row per order, with its items and payments reconciled.
--
-- The three money columns are kept SEPARATE on purpose:
--   items_total    what the line items add up to
--   settled_value  what was actually paid, refunds excluded
--   refunded_value what was given back
--
-- Collapsing them into one "revenue" column is what makes a refund look like
-- a data-quality failure. Keeping them apart lets net revenue be derived
-- correctly and lets the consistency tests compare the right pair.
{{ config(materialized='table') }}

with item_rollup as (
    select
        order_id,
        count(*)              as item_count,
        sum(quantity)         as units,
        sum(line_total)       as items_total,
        sum(discount)         as items_discount
    from {{ ref('stg_order_items') }}
    group by order_id
),

payment_rollup as (
    select
        order_id,
        count(*)                          as payment_count,
        count(*) filter (where is_refund) as refund_count,
        sum(settled_value)                as settled_value,
        sum(refunded_value)               as refunded_value
    from {{ ref('stg_payments') }}
    group by order_id
)

select
    o.order_id,
    o.customer_id,
    c.segment                                   as customer_segment,
    c.state                                     as customer_state,
    o.order_status,
    o.is_fulfilled,
    o.order_date,
    o.order_date::date                          as order_day,
    o.approved_at,
    o.delivered_at,
    o.delivery_days,
    o.shipping_state,
    o.total_amount,
    o.freight_amount,
    o.discount_amount,
    coalesce(i.item_count, 0)                   as item_count,
    coalesce(i.units, 0)                        as units,
    round(coalesce(i.items_total, 0), 2)        as items_total,
    round(coalesce(i.items_discount, 0), 2)     as items_discount,
    coalesce(p.payment_count, 0)                as payment_count,
    coalesce(p.refund_count, 0)                 as refund_count,
    round(coalesce(p.settled_value, 0), 2)      as settled_value,
    round(coalesce(p.refunded_value, 0), 2)     as refunded_value,
    round(coalesce(p.settled_value, 0) - coalesce(p.refunded_value, 0), 2)
                                                as net_revenue,
    coalesce(p.refund_count, 0) > 0             as was_refunded,
    -- Cancelled orders are genuinely never paid, so "unpaid" is only
    -- meaningful for the rest.
    o.order_status <> 'cancelled' and coalesce(p.payment_count, 0) = 0
                                                as is_unpaid,
    o.ingested_at,
    o.batch_id
from {{ ref('stg_orders') }} o
join {{ ref('stg_customers') }} c on c.customer_id = o.customer_id
left join item_rollup i    on i.order_id = o.order_id
left join payment_rollup p on p.order_id = o.order_id
