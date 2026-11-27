-- Analytics: customer dimension with order-derived attributes.
{{ config(materialized='table') }}

with order_stats as (
    select
        customer_id,
        count(*)                              as orders_total,
        count(*) filter (where is_fulfilled)  as orders_fulfilled,
        count(*) filter (where order_status = 'cancelled') as orders_cancelled,
        count(*) filter (where order_status = 'returned')  as orders_returned,
        min(order_date)                       as first_order_at,
        max(order_date)                       as last_order_at,
        sum(total_amount)                     as gross_order_value,
        avg(total_amount)                     as avg_order_value
    from {{ ref('stg_orders') }}
    group by customer_id
)

select
    c.customer_id,
    c.customer_unique_id,
    c.first_name,
    c.last_name,
    c.email,
    c.city,
    c.state,
    c.country,
    c.segment,
    c.created_at,
    coalesce(s.orders_total, 0)      as orders_total,
    coalesce(s.orders_fulfilled, 0)  as orders_fulfilled,
    coalesce(s.orders_cancelled, 0)  as orders_cancelled,
    coalesce(s.orders_returned, 0)   as orders_returned,
    s.first_order_at,
    s.last_order_at,
    round(coalesce(s.gross_order_value, 0), 2) as gross_order_value,
    round(coalesce(s.avg_order_value, 0), 2)   as avg_order_value,
    -- A customer with no orders is a real state, not missing data.
    coalesce(s.orders_total, 0) > 0 as has_ordered
from {{ ref('stg_customers') }} c
left join order_stats s on s.customer_id = c.customer_id
