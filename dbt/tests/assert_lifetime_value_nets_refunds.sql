-- Lifetime value must be gross minus refunds, per customer.
--
-- Guards against the aggregate drifting back to a gross figure, which would
-- overstate every customer who ever returned something.
select
    l.customer_id,
    l.gross_value,
    l.refunded_value,
    l.lifetime_value,
    o.expected_net
from {{ ref('agg_customer_lifetime') }} l
join (
    select customer_id, round(sum(net_revenue), 2) as expected_net
    from {{ ref('fct_orders') }}
    group by customer_id
) o on o.customer_id = l.customer_id
where {{ money_differs('l.lifetime_value', 'o.expected_net') }}
