-- No event may be dated in the future.
--
-- One hour of slack for ordinary clock skew between the generator, the
-- database and the scheduler.
select order_id, order_date, now() as checked_at
from {{ ref('fct_orders') }}
where order_date > now() + interval '1 hour'
