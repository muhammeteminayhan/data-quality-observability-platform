-- Analytics: product dimension with sales-derived attributes.
{{ config(materialized='table') }}

with item_stats as (
    select
        product_id,
        count(*)                       as times_ordered,
        sum(quantity)                  as units_sold,
        sum(line_total)                as revenue,
        avg(unit_price)                as avg_selling_price,
        count(distinct order_id)       as distinct_orders
    from {{ ref('stg_order_items') }}
    group by product_id
)

select
    p.product_id,
    p.product_name,
    p.category,
    p.subcategory,
    p.price                              as list_price,
    p.cost,
    p.margin_rate,
    p.weight_kg,
    p.is_active,
    p.created_at,
    coalesce(s.times_ordered, 0)         as times_ordered,
    coalesce(s.units_sold, 0)            as units_sold,
    round(coalesce(s.revenue, 0), 2)     as revenue,
    round(coalesce(s.avg_selling_price, p.price), 2) as avg_selling_price,
    coalesce(s.distinct_orders, 0)       as distinct_orders,
    -- A sold price materially below list means discounting, which is worth
    -- surfacing as a dimension attribute rather than recomputing downstream.
    case
        when s.avg_selling_price is null then null
        else round((p.price - s.avg_selling_price) / nullif(p.price, 0), 4)
    end as avg_discount_rate
from {{ ref('stg_products') }} p
left join item_stats s on s.product_id = p.product_id
