-- Analytics: customer lifetime value, netted of refunds.
{{ config(materialized='table') }}

select
    c.customer_id,
    c.segment,
    c.state,
    c.created_at,
    count(o.order_id)                            as orders_total,
    count(o.order_id) filter (where o.is_fulfilled) as orders_fulfilled,
    count(o.order_id) filter (where o.was_refunded) as orders_refunded,
    min(o.order_date)                            as first_order_at,
    max(o.order_date)                            as last_order_at,
    round(coalesce(sum(o.items_total), 0), 2)    as gross_value,
    round(coalesce(sum(o.refunded_value), 0), 2) as refunded_value,
    -- Lifetime value nets refunds. Reporting gross as "value" is the kind of
    -- quietly wrong number this platform exists to prevent.
    round(coalesce(sum(o.net_revenue), 0), 2)    as lifetime_value,
    round(
        coalesce(sum(o.net_revenue), 0) / nullif(count(o.order_id), 0), 2
    )                                            as avg_net_order_value,
    case
        when count(o.order_id) = 0 then 'no_orders'
        when count(o.order_id) = 1 then 'one_off'
        when count(o.order_id) <= 4 then 'repeat'
        else 'loyal'
    end                                          as lifecycle_stage
from {{ ref('dim_customers') }} c
left join {{ ref('fct_orders') }} o on o.customer_id = c.customer_id
group by c.customer_id, c.segment, c.state, c.created_at
