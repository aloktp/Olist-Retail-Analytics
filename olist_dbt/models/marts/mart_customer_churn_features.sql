-- ============================================================
-- PURPOSE:
-- Final customer-level dataset for churn prediction (ML-ready)
--
-- This is the MOST IMPORTANT table in the project.
--
-- It is used for:
-- - Machine learning model (churn prediction)
-- - Business analysis of churn drivers
-- - Identifying high-risk customers
-- ============================================================

-- ============================================================
-- 1) LOAD CUSTOMER-LEVEL DATA
--    This already contains aggregated behaviour, delivery,
--    and experience metrics + churn label
-- ============================================================

with customers as (

    select *
    from {{ ref('int_customer_orders') }}

),

-- ============================================================
-- 2) FEATURE ENGINEERING
--    Create additional features for ML model
-- ============================================================

final as (

    select
        customer_unique_id,
        customer_state,

        -- -------------------------
        -- ORDER / VALUE FEATURES
        -- -------------------------
        total_orders,
        lifetime_value,
        avg_order_value,

        -- -------------------------
        -- DELIVERY FEATURES
        -- -------------------------
        avg_delivery_days,
        avg_delivery_delay,
        pct_orders_late,

        -- -------------------------
        -- CUSTOMER EXPERIENCE
        -- -------------------------
        avg_review_score,
        pct_low_reviews,

        -- -------------------------
        -- RECENCY (VERY IMPORTANT)
        -- -------------------------
        days_since_last_order,

        -- -------------------------
        -- INTERACTION FEATURE
        -- Combines delay + dissatisfaction
        -- Higher value = worse experience
        -- -------------------------
        round(avg_delivery_delay * (5 - avg_review_score), 4) 
            as delay_dissatisfaction_score,

        -- -------------------------
        -- TARGET VARIABLE
        -- -------------------------
        is_churned,

        current_timestamp() as _model_run_at

    from customers
    where customer_unique_id is not null
)

-- ============================================================
-- FINAL OUTPUT
-- One row per customer (ML-ready dataset)
-- ============================================================

select *
from final