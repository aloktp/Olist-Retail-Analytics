-- ============================================================
-- PURPOSE:
-- Create seller-level performance dataset
--
-- This helps answer:
-- - Which sellers are causing delays?
-- - Which sellers drive poor customer experience?
-- - Are a small number of sellers responsible for churn drivers?

-- i.e. We can find out which sellers are the worst performers.

-- This is critical for:
-- - Operational improvements
-- - Seller risk classification
-- - Business decision-making (RETAIN/REMOVE sellers i.e. where should Business intervene)
-- ============================================================

-- ============================================================
-- 1) LOAD ENRICHED ORDER DATA
--    This table already contains:
--    - delivery metrics
--    - order value
--    - review scores
-- ============================================================

with orders as (

    select *
    from {{ ref('int_orders_enriched') }}
    where seller_id is not null   -- exclude records without seller info

),

-- ============================================================
-- 2) SELLER-LEVEL AGGREGATION
--    Goal: Summarize each seller's performance
-- ============================================================

seller_agg as (

    select
        seller_id,

        -- -------------------------
        -- ORDER / BUSINESS METRICS
        -- -------------------------
        count(order_id) as total_orders,                   -- total orders fulfilled
        count(distinct customer_id) as unique_customers,   -- customer reach
        sum(order_gross_value) as total_gmv,               -- total revenue generated

        -- -------------------------
        -- DELIVERY PERFORMANCE
        -- -------------------------
        avg(total_delivery_days) as avg_delivery_days,
        avg(delivery_delay_days) as avg_delay_days,

        -- number of late deliveries
        sum(is_late_delivery) as late_order_count,

        -- % of orders delivered late
        round(sum(is_late_delivery)*100.0/count(order_id),2) as late_delivery_rate,

        -- -------------------------
        -- CUSTOMER EXPERIENCE
        -- -------------------------
        avg(review_score) as avg_review_score,
        sum(is_low_satisfaction) as low_review_count,

        -- % of bad reviews
        round(sum(is_low_satisfaction)*100.0/count(order_id),2) as pct_low_reviews,

        -- -------------------------
        -- SELLER RISK CLASSIFICATION
        -- Based on late delivery rate
        -- -------------------------
        case
            when round(sum(is_late_delivery)*100.0/count(order_id),2) >= 40 then 'High Risk'
            when round(sum(is_late_delivery)*100.0/count(order_id),2) >= 20 then 'Medium Risk'
            else 'Low Risk'
        end as seller_risk_tier

    from orders
    group by seller_id

    -- remove low-volume sellers (avoid noisy metrics)
    having count(order_id) >= 5
)

-- ============================================================
-- FINAL OUTPUT
-- One row per seller with performance + risk classification
-- ============================================================

select *
from seller_agg