-- Staging: payments
--
-- payment_value is NOT forced positive: a refund is a legitimate negative
-- payment. The sign is instead checked against the payment TYPE, which is the
-- rule that actually holds.
{{ config(materialized='table') }}

with typed as (

    select
        ingestion_id,
        nullif(trim(payment_id), '')          as payment_id,
        nullif(trim(order_id), '')            as order_id,
        lower(nullif(trim(payment_type), '')) as payment_type,
        payment_sequence,
        payment_installments,
        payment_value,
        payment_date,
        _ingested_at                          as ingested_at,
        _batch_id                             as batch_id
    from {{ source('raw', 'payments') }}

), keyed as (

    select * from typed
    where payment_id is not null
      and order_id is not null
      and payment_value is not null
      and payment_type in ({{ "'" ~ var('payment_types') | join("','") ~ "'" }})
      -- Sign must agree with the type. A positive refund or a negative
      -- settlement is a sign-handling bug upstream and would corrupt every
      -- revenue total that nets them.
      and (
          (payment_type = 'refund' and payment_value < 0)
          or (payment_type <> 'refund' and payment_value > 0)
      )

), deduplicated as (

    select * from (
        select *, {{ dedupe_by('payment_id') }} as is_current
        from keyed
    ) ranked_rows
    where is_current

)

select
    p.payment_id,
    p.order_id,
    p.payment_type,
    p.payment_sequence,
    p.payment_installments,
    p.payment_value,
    p.payment_type = 'refund' as is_refund,
    case when p.payment_type <> 'refund' then p.payment_value else 0 end
        as settled_value,
    case when p.payment_type = 'refund' then -p.payment_value else 0 end
        as refunded_value,
    p.payment_date,
    p.ingested_at,
    p.batch_id,
    p.ingestion_id as source_ingestion_id
from deduplicated p
join {{ ref('stg_orders') }} o on o.order_id = p.order_id
