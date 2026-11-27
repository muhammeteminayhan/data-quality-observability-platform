-- Staging: customers
--
-- The raw layer accepts anything. This model is the first place the data is
-- allowed to be clean, so it does three things the raw layer deliberately
-- does not: it drops rows with no business key, it removes duplicates, and it
-- normalises the text fields.
--
-- Rejected rows are NOT silently discarded - stg_rejected_rows records them
-- with a reason, so a drop is observable rather than invisible.
{{ config(materialized='table') }}

with ranked as (

    select
        ingestion_id,
        nullif(trim(customer_id), '')         as customer_id,
        nullif(trim(customer_unique_id), '')  as customer_unique_id,
        nullif(trim(first_name), '')          as first_name,
        nullif(trim(last_name), '')           as last_name,
        lower(nullif(trim(email), ''))        as email,
        nullif(trim(phone), '')               as phone,
        nullif(trim(city), '')                as city,
        upper(nullif(trim(state), ''))        as state,
        nullif(trim(zip_code), '')            as zip_code,
        upper(nullif(trim(country), ''))      as country,
        lower(nullif(trim(segment), ''))      as segment,
        created_at,
        _ingested_at                          as ingested_at,
        _batch_id                             as batch_id
    from {{ source('raw', 'customers') }}

), keyed as (

    select *
    from ranked
    -- No business key means the row cannot be referenced by anything, so it
    -- cannot participate in the model at all.
    where customer_id is not null

), deduplicated as (

    select *
    from (
        select
            *,
            {{ dedupe_by('customer_id') }} as is_current
        from keyed
    ) ranked_rows
    where is_current

)

select
    customer_id,
    customer_unique_id,
    first_name,
    last_name,
    email,
    phone,
    city,
    state,
    zip_code,
    coalesce(country, 'US') as country,
    segment,
    created_at,
    ingested_at,
    batch_id,
    -- Carried forward so the analytics layer and the GX suites can tell a
    -- cleaned row from the raw one it came from.
    ingestion_id as source_ingestion_id
from deduplicated
