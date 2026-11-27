-- A line total must equal quantity * unit_price - discount.
--
-- Within the project rounding tolerance: each component is rounded to cents
-- independently, so an exact equality test fails on correct data.
select
    order_item_id,
    quantity,
    unit_price,
    discount,
    line_total,
    expected_line_total,
    line_total_delta
from {{ ref('fct_order_items') }}
where {{ money_differs('line_total', 'expected_line_total') }}
