-- Staging: what the staging layer threw away, and why.
--
-- Why this model exists
-- ---------------------
-- Every other staging model filters rows out. A filter is invisible: the
-- analytics layer simply sees fewer rows and cannot tell a quiet drop from a
-- quiet upstream outage. This model makes the drops explicit and countable,
-- so "staging rejected 40 customers today" becomes a number on a dashboard
-- rather than a silence.
--
-- It is the difference between enforcing quality and hiding a problem.
{{ config(materialized='table') }}

with customer_rejects as (
    select
        'raw.customers' as source_table,
        r.ingestion_id,
        coalesce(r.customer_id, '(null)') as business_key,
        case
            when nullif(trim(r.customer_id), '') is null then 'missing_customer_id'
            else 'duplicate_customer_id'
        end as reject_reason
    from {{ source('raw', 'customers') }} r
    where not exists (
        select 1 from {{ ref('stg_customers') }} s
        where s.source_ingestion_id = r.ingestion_id
    )
),

product_rejects as (
    select
        'raw.products' as source_table,
        r.ingestion_id,
        coalesce(r.product_id, '(null)') as business_key,
        case
            when nullif(trim(r.product_id), '') is null then 'missing_product_id'
            when r.price is null                        then 'missing_price'
            when r.price <= 0                           then 'non_positive_price'
            else 'duplicate_product_id'
        end as reject_reason
    from {{ source('raw', 'products') }} r
    where not exists (
        select 1 from {{ ref('stg_products') }} s
        where s.source_ingestion_id = r.ingestion_id
    )
),

order_rejects as (
    select
        'raw.orders' as source_table,
        r.ingestion_id,
        coalesce(r.order_id, '(null)') as business_key,
        case
            when nullif(trim(r.order_id), '') is null    then 'missing_order_id'
            when nullif(trim(r.customer_id), '') is null then 'missing_customer_id'
            when r.order_date is null                    then 'missing_order_date'
            when r.total_amount is null                  then 'missing_total_amount'
            when r.total_amount < 0                      then 'negative_total_amount'
            when lower(trim(r.order_status)) not in
                 ({{ "'" ~ var('order_statuses') | join("','") ~ "'" }})
                                                         then 'unknown_order_status'
            when r.order_date > now() + interval '1 hour' then 'future_order_date'
            else 'orphan_customer_or_duplicate'
        end as reject_reason
    from {{ source('raw', 'orders') }} r
    where not exists (
        select 1 from {{ ref('stg_orders') }} s
        where s.source_ingestion_id = r.ingestion_id
    )
),

item_rejects as (
    select
        'raw.order_items' as source_table,
        r.ingestion_id,
        coalesce(r.order_item_id, '(null)') as business_key,
        case
            when nullif(trim(r.order_item_id), '') is null then 'missing_order_item_id'
            when r.quantity is null or r.quantity <= 0     then 'non_positive_quantity'
            when r.unit_price is null or r.unit_price <= 0  then 'non_positive_unit_price'
            when r.line_total is null                      then 'missing_line_total'
            else 'orphan_parent_or_duplicate'
        end as reject_reason
    from {{ source('raw', 'order_items') }} r
    where not exists (
        select 1 from {{ ref('stg_order_items') }} s
        where s.source_ingestion_id = r.ingestion_id
    )
),

payment_rejects as (
    select
        'raw.payments' as source_table,
        r.ingestion_id,
        coalesce(r.payment_id, '(null)') as business_key,
        case
            when nullif(trim(r.payment_id), '') is null then 'missing_payment_id'
            when r.payment_value is null                then 'missing_payment_value'
            when lower(trim(r.payment_type)) not in
                 ({{ "'" ~ var('payment_types') | join("','") ~ "'" }})
                                                        then 'unknown_payment_type'
            when lower(trim(r.payment_type)) = 'refund' and r.payment_value >= 0
                                                        then 'non_negative_refund'
            when lower(trim(r.payment_type)) <> 'refund' and r.payment_value <= 0
                                                        then 'non_positive_settlement'
            else 'orphan_order_or_duplicate'
        end as reject_reason
    from {{ source('raw', 'payments') }} r
    where not exists (
        select 1 from {{ ref('stg_payments') }} s
        where s.source_ingestion_id = r.ingestion_id
    )
)

select source_table, ingestion_id, business_key, reject_reason, now() as detected_at
from (
    select * from customer_rejects
    union all select * from product_rejects
    union all select * from order_rejects
    union all select * from item_rejects
    union all select * from payment_rejects
) all_rejects
