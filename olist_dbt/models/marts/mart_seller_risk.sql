-- ============================================================
-- PURPOSE:
-- Final seller risk table for business decision-making
--
-- Helps identify:
-- - High-risk sellers (late deliveries, poor reviews)
-- - Operational bottlenecks
-- ============================================================

-- ============================================================
-- 1) LOAD SELLER PERFORMANCE DATA
-- ============================================================

with sellers as (

    select *
    from {{ ref('int_seller_performance') }}

),

-- ============================================================
-- 2) LOAD SELLER LOCATION (for reporting)
-- ============================================================

seller_locations as (

    select
        seller_id,
        seller_city,
        seller_state
    from {{ ref('stg_sellers') }}

),

-- ============================================================
-- 3) FINAL OUTPUT WITH RANKING
-- ============================================================

final as (

    select
        s.seller_id,
        sl.seller_city,
        sl.seller_state,

        -- BUSINESS METRICS
        s.total_orders,
        s.total_gmv,
        s.avg_delivery_days,
        s.late_delivery_rate,
        s.avg_review_score,
        s.pct_low_reviews,

        -- RISK CLASSIFICATION
        s.seller_risk_tier,

        -- ranking sellers within each risk tier
        row_number() over (
            partition by s.seller_risk_tier
            order by s.late_delivery_rate desc
        ) as risk_rank_in_tier

    from sellers s
    left join seller_locations sl
        on s.seller_id = sl.seller_id

)

select *
from final