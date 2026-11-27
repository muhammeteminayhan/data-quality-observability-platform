-- Referential integrity across the analytics layer.
--
-- The relationships tests cover each edge individually; this one catches the
-- case where both sides exist but the join itself drops rows, which a
-- per-column test cannot see.
select o.order_id, o.customer_id
from {{ ref('fct_orders') }} o
left join {{ ref('dim_customers') }} c on c.customer_id = o.customer_id
where c.customer_id is null
