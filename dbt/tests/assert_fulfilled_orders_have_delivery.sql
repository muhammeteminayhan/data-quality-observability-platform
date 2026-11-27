-- A shipped or delivered order should carry a delivery timestamp.
--
-- Severity warn rather than error: a fulfilment feed can legitimately lag by
-- a run, so this should be visible without failing the build.
{{ config(severity='warn') }}

select order_id, order_status, delivered_at
from {{ ref('fct_orders') }}
where is_fulfilled and delivered_at is null
