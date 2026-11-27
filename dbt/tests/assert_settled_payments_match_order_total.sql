-- Settled payments must equal the order total.
--
-- Refunds are excluded via settled_value, and cancelled orders are excluded
-- because they are genuinely never paid. On this project's own clean data a
-- naive sum over ALL payment rows raises 3,265 false positives out of 50,000
-- orders - every one of them a legitimate refund - which is exactly why the
-- fact model keeps settled_value and refunded_value apart.
select
    order_id,
    order_status,
    total_amount,
    settled_value,
    refunded_value,
    round(total_amount - settled_value, 2) as delta
from {{ ref('fct_orders') }}
where order_status <> 'cancelled'
  and payment_count > 0
  and {{ money_differs('total_amount', 'settled_value') }}
