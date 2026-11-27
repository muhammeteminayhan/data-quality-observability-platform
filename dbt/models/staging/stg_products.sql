-- Staging: products
{{ config(materialized='table') }}

with typed as (

    select
        ingestion_id,
        nullif(trim(product_id), '')        as product_id,
        nullif(trim(product_name), '')      as product_name,
        lower(nullif(trim(category), ''))   as category,
        lower(nullif(trim(subcategory), '')) as subcategory,
        price,
        cost,
        weight_kg,
        coalesce(is_active, true)           as is_active,
        created_at,
        _ingested_at                        as ingested_at,
        _batch_id                           as batch_id
    from {{ source('raw', 'products') }}

), keyed as (

    select * from typed
    where product_id is not null
      -- A non-positive price makes every downstream revenue figure wrong, so
      -- the row is excluded here rather than quietly poisoning the margins.
      and price is not null
      and price > 0

), deduplicated as (

    select * from (
        select *, {{ dedupe_by('product_id') }} as is_current
        from keyed
    ) ranked_rows
    where is_current

)

select
    product_id,
    product_name,
    category,
    subcategory,
    price,
    cost,
    case
        when cost is null or price = 0 then null
        else round((price - cost) / price, 4)
    end as margin_rate,
    weight_kg,
    is_active,
    created_at,
    ingested_at,
    batch_id,
    ingestion_id as source_ingestion_id
from deduplicated
