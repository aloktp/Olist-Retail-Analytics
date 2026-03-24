-- ============================================================
-- PURPOSE:
-- State-level delivery performance analysis
--
-- Helps identify:
-- - Regions with poor delivery performance
-- - Correlation between delays and satisfaction
-- ============================================================

-- ============================================================
-- 1) LOAD ORDER DATA
-- ============================================================

with orders as (

    select *
    from {{ ref('int_orders_enriched') }}

),

-- ============================================================
-- 2) LOAD CUSTOMER LOCATION
-- ============================================================

customers as (

    select
        customer_id,
        customer_state
    from {{ ref('stg_customers') }}

),

-- ============================================================
-- 3) STATE-LEVEL AGGREGATION
-- ============================================================

state_perf as (

    select
        c.customer_state,

        -- VOLUME
        count(o.order_id) as total_orders,

        -- DELIVERY METRICS
        avg(o.total_delivery_days) as avg_delivery_days,
        avg(o.delivery_delay_days) as avg_delay_days,
        round(sum(o.is_late_delivery)*100.0/count(o.order_id),2) as late_rate_pct,

        -- CUSTOMER EXPERIENCE
        avg(o.review_score) as avg_review_score

    from orders o
    left join customers c
        on o.customer_id = c.customer_id

    group by c.customer_state

)

select *
from state_perf
order by late_rate_pct desc