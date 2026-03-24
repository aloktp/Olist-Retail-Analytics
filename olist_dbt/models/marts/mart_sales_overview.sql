-- ============================================================
-- PURPOSE:
-- Monthly business performance overview
--
-- Used for:
-- - Revenue trends
-- - Order growth
-- - Delivery performance tracking
-- ============================================================

with orders as (

    select *
    from {{ ref('int_orders_enriched') }}

),

monthly as (

    select
        date_trunc('month', order_purchased_at) as order_month,

        -- BUSINESS KPIs
        count(order_id) as total_orders,
        count(distinct customer_id) as unique_customers,
        sum(order_gross_value) as total_revenue,
        avg(order_gross_value) as avg_order_value,

        -- DELIVERY PERFORMANCE
        sum(is_late_delivery) as late_orders,
        round(sum(is_late_delivery)*100.0/count(order_id),2) as late_delivery_rate_pct,

        -- CUSTOMER EXPERIENCE
        avg(review_score) as avg_review_score

    from orders
    group by date_trunc('month', order_purchased_at)

)

select *
from monthly
order by order_month