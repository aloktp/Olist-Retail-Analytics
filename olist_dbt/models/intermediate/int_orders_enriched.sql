-- ============================================================
-- PURPOSE:
-- Order-level dataset combining:
-- - orders
-- - items (1-to-many)
-- - payments (1-to-many)
-- - reviews (1-to-many)
--
-- Handles:
-- - aggregation of 1-to-many relationships
-- - deduplication of reviews using window function
-- ============================================================

with orders as (

    select *
    from {{ ref('stg_orders') }}
    where is_delivered = 1

),

-- ============================================================
-- Aggregate order items
-- (order_id, order_item_id) → aggregated to order level
-- ============================================================

items as (

    select
        order_id,
        count(order_item_id) as item_count,
        sum(item_price) as items_subtotal,
        sum(freight_value) as freight_total,
        sum(item_total_value) as order_gross_value,
        min(seller_id) as primary_seller_id

    from {{ ref('stg_order_items') }}
    group by order_id
),

-- ============================================================
-- Aggregate payments
-- (order_id, payment_sequential) → aggregated to order level
-- ============================================================

payments as (

    select
        order_id,
        sum(payment_value) as total_payment_value,
        count(distinct payment_type) as payment_method_count,
        max(payment_installments) as max_installments

    from {{ ref('stg_order_payments') }}
    group by order_id
),

-- ============================================================
-- Deduplicate reviews (keep latest per order)
-- ============================================================

reviews as (

    select 
        order_id, 
        review_score, 
        is_low_satisfaction
    from (
        select *,
               row_number() over (
                   partition by order_id 
                   order by review_created_at desc
               ) as rn
        from {{ ref('stg_order_reviews') }}
    )
    where rn = 1
),

-- ============================================================
-- Final dataset
-- ============================================================

final as (

    select
        o.order_id,
        o.customer_id,
        o.order_status,
        o.order_purchased_at,
        o.delivered_at,
        o.estimated_delivery_at,

        -- DELIVERY METRICS
        datediff(o.delivered_at, o.estimated_delivery_at) as delivery_delay_days,

        case 
            when o.delivered_at > o.estimated_delivery_at then 1 
            else 0 
        end as is_late_delivery,

        datediff(o.delivered_at, o.order_purchased_at) as total_delivery_days,

        -- VALUE METRICS
        i.item_count,
        i.order_gross_value,
        p.total_payment_value,
        p.payment_method_count,
        p.max_installments,
        i.primary_seller_id as seller_id,

        -- REVIEW METRICS
        r.review_score,
        r.is_low_satisfaction

    from orders o
    left join items i on o.order_id = i.order_id
    left join payments p on o.order_id = p.order_id
    left join reviews r on o.order_id = r.order_id
)

select * 
from final