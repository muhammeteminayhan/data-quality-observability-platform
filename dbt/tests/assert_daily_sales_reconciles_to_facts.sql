-- The daily aggregate must reconcile to the fact table it is built from.
--
-- An aggregate that silently disagrees with its own grain is the classic
-- warehouse failure: every dashboard reads the aggregate, nobody re-derives
-- it, and the error is invisible until someone totals it by hand.
with aggregated as (
    select
        sales_date,
        sum(net_amount) as agg_net,
        sum(units_sold) as agg_units
    from {{ ref('agg_daily_sales') }}
    group by sales_date
),
from_facts as (
    select
        order_day as sales_date,
        round(sum(line_total), 2) as fact_net,
        sum(quantity)             as fact_units
    from {{ ref('fct_order_items') }}
    group by order_day
)
select
    a.sales_date,
    a.agg_net,
    f.fact_net,
    a.agg_units,
    f.fact_units
from aggregated a
full outer join from_facts f on f.sales_date = a.sales_date
where a.sales_date is null
   or f.sales_date is null
   or {{ money_differs('a.agg_net', 'f.fact_net') }}
   or coalesce(a.agg_units, -1) <> coalesce(f.fact_units, -1)
