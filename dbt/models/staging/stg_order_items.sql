-- Staging: order items
{{ config(materialized='table') }}

with typed as (

    select
        ingestion_id,
        nullif(trim(order_item_id), '') as order_item_id,
        nullif(trim(order_id), '')      as order_id,
        nullif(trim(product_id), '')    as product_id,
        nullif(trim(seller_id), '')     as seller_id,
        item_sequence,
        quantity,
        unit_price,
        coalesce(discount, 0)           as discount,
        line_total,
        shipping_limit_date,
        _ingested_at                    as ingested_at,
        _batch_id                       as batch_id
    from {{ source('raw', 'order_items') }}

), keyed as (

    select * from typed
    where order_item_id is not null
      and order_id is not null
      and product_id is not null
      -- A zero or negative quantity makes the line total meaningless.
      and quantity is not null and quantity > 0
      and unit_price is not null and unit_price > 0
      and line_total is not null

), deduplicated as (

    select * from (
        select *, {{ dedupe_by('order_item_id') }} as is_current
        from keyed
    ) ranked_rows
    where is_current

)

select
    i.order_item_id,
    i.order_id,
    i.product_id,
    i.seller_id,
    i.item_sequence,
    i.quantity,
    i.unit_price,
    i.discount,
    i.line_total,
    -- Recomputed from its parts. Keeping both lets the singular test compare
    -- what the source claimed against what the components imply.
    round(i.quantity * i.unit_price - i.discount, 2) as expected_line_total,
    i.shipping_limit_date,
    i.ingested_at,
    i.batch_id,
    i.ingestion_id as source_ingestion_id
from deduplicated i
join {{ ref('stg_orders') }} o   on o.order_id = i.order_id
join {{ ref('stg_products') }} p on p.product_id = i.product_id
