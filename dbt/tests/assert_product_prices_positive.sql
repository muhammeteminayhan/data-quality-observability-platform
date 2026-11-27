-- Every product reaching the analytics layer must have a usable price.
select product_id, list_price, cost
from {{ ref('dim_products') }}
where list_price is null or list_price <= 0
