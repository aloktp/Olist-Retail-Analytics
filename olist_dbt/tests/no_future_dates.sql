-- Check whether order purchased date is not a far future date.

select *
from {{ ref('int_orders_enriched') }}
where order_purchased_at > current_date