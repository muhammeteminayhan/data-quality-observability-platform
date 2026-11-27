-- Analytics: daily sales by category.
--
-- This is the warehouse counterpart of the daily_sales Parquet dataset in the
-- lake, built from the same source rows. That makes the cross-store
-- comparison in tests/assert_warehouse_matches_lake.sql a genuine check of
-- two independent paths rather than a tautology.
{{ config(materialized='table') }}

select
    i.order_day                                as sales_date,
    i.category,
    i.subcategory,
    count(distinct i.order_id)                 as order_count,
    count(*)                                   as line_count,
    sum(i.quantity)                            as units_sold,
    round(sum(i.quantity * i.unit_price), 2)   as gross_amount,
    round(sum(i.discount), 2)                  as discount_amount,
    round(sum(i.line_total), 2)                as net_amount,
    round(sum(i.gross_profit), 2)              as gross_profit,
    round(sum(i.line_total) / nullif(count(distinct i.order_id), 0), 2)
                                               as avg_order_value
from {{ ref('fct_order_items') }} i
group by i.order_day, i.category, i.subcategory
