-- An order total must equal its line items plus freight.
--
-- Orders with no surviving line items are excluded: that is a referential
-- problem which the relationships test already reports, and counting it here
-- too would open two incidents for one cause.
select
    order_id,
    total_amount,
    items_total,
    freight_amount,
    round(total_amount - (items_total + freight_amount), 2) as delta
from {{ ref('fct_orders') }}
where item_count > 0
  and {{ money_differs('total_amount', 'items_total + freight_amount') }}
