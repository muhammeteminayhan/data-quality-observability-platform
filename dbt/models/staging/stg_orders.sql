-- Staging: orders
--
-- Referential integrity is enforced HERE, not in the raw layer: an order
-- pointing at a customer that does not exist is dropped, because every
-- downstream aggregate would otherwise attribute revenue to nobody.
{{ config(materialized='table') }}

with typed as (

    select
        ingestion_id,
        nullif(trim(order_id), '')            as order_id,
        nullif(trim(customer_id), '')         as customer_id,
        lower(nullif(trim(order_status), '')) as order_status,
        order_date,
        approved_at,
        delivered_at,
        nullif(trim(shipping_city), '')       as shipping_city,
        upper(nullif(trim(shipping_state), '')) as shipping_state,
        total_amount,
        coalesce(freight_amount, 0)           as freight_amount,
        coalesce(discount_amount, 0)          as discount_amount,
        _ingested_at                          as ingested_at,
        _batch_id                             as batch_id
    from {{ source('raw', 'orders') }}

), keyed as (

    select * from typed
    where order_id is not null
      and customer_id is not null
      and order_date is not null
      and total_amount is not null
      and total_amount >= 0
      -- An unknown status cannot be reasoned about, so the row is excluded
      -- rather than silently bucketed as something it is not.
      and order_status in ({{ "'" ~ var('order_statuses') | join("','") ~ "'" }})
      -- A future order date is a clock or extraction fault.
      and order_date <= now() + interval '1 hour'

), deduplicated as (

    select * from (
        select *, {{ dedupe_by('order_id') }} as is_current
        from keyed
    ) ranked_rows
    where is_current

)

select
    o.order_id,
    o.customer_id,
    o.order_status,
    o.order_date,
    o.approved_at,
    o.delivered_at,
    o.shipping_city,
    o.shipping_state,
    o.total_amount,
    o.freight_amount,
    o.discount_amount,
    o.order_status in ({{ "'" ~ var('fulfilled_statuses') | join("','") ~ "'" }})
        as is_fulfilled,
    case
        when o.delivered_at is not null and o.order_date is not null
        then extract(epoch from (o.delivered_at - o.order_date)) / 86400.0
    end as delivery_days,
    o.ingested_at,
    o.batch_id,
    o.ingestion_id as source_ingestion_id
from deduplicated o
-- Referential integrity against the cleaned customer dimension, so an order
-- can only survive if its customer survived too.
join {{ ref('stg_customers') }} c on c.customer_id = o.customer_id
