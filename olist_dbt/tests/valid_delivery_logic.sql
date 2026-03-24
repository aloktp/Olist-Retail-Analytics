-- Check whether delivery date is before order purchase date

select *
from {{ ref('int_orders_enriched') }}
where delivered_at < order_purchased_at