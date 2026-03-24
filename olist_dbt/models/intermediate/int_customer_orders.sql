-- We are going to create Customer-leel dataset to get churn logic.
-- We are trying to answer Who churned ?, Why they churned? and What their behaviour was?
-- Churn means customer who was previously active, has now been inactive for a defined period = 180 days, and its assumed is unlikely to return.
-- We already created behaviour, delivery and satisfaction predictors in Orders enriched i.e. Order level dataset
-- With this Customer leel dataset view, we can get :-
/*
1.) Customer segmentation
High value vs low value
Fast delivery vs slow delivery
Happy vs unhappy customers

2.) Revenue analysis
Lifetime value (LTV)
Order frequency
Recency

3.) Experience diagnostics
% late deliveries
% low reviews
Average delivery delay

*/

-- ============================================================
-- 1) LOAD BASE ORDER DATA (already enriched at order level)
--    This table has one row per order with delivery metrics,
--    value metrics, review info, etc.
-- ============================================================

with orders as (
    select * from {{ ref('int_orders_enriched') }}
),

-- ============================================================
-- 2) LOAD CUSTOMER DIMENSION
--    We need customer_unique_id because customer_id in Olist
--    is NOT stable — same person can appear multiple times.
-- ============================================================

customers as (
    select
        customer_id,
        customer_unique_id,
        customer_state
    from {{ ref('stg_customers') }}
),

-- ============================================================
-- 3) ATTACH CUSTOMER UNIQUE ID TO EACH ORDER
--    - In Olist, customer_id is NOT stable (same person can appear multiple times)
--    - customer_unique_id is the true person-level identifier
--    - Some orders may have duplicate customer_id mappings due to data quirks
--    - So, we Join orders to customers to bring in customer_unique_id
--    - Use row_number() to ensure ONE row per order_id after the join
-- ============================================================

orders_with_unique_id as (

    select *
    from (
        select
            -- Bring in the stable customer identifier
            c.customer_unique_id,
            c.customer_state,

            -- Bring all order-level fields
            o.*,

            -- Deduplicate in case the join produces multiple matches
            row_number() over (
                partition by o.order_id
                order by c.customer_unique_id
            ) as rn

        from orders o
        left join customers c
            on o.customer_id = c.customer_id
    )

    -- Keep only the first match per order_id
    where rn = 1
),

-- ============================================================
-- 4) CUSTOMER-LEVEL AGGREGATION
--    This is the MAIN table.
--    Goal: Summarize each customer's entire lifecycle:
--      - order behaviour
--      - value
--      - delivery experience
--      - satisfaction
--      - recency (for churn calculation)
--      - churn flag (for churn logic)

-- 	  FIX for duplicate rows because of duplicate customer ids having different "State" where customer lives in, after customers was joined with orders:
--    - i.e. Customers previously lived in one state, and later lived in another state, and updated data in their Olist profile.
--    - Ensure ONE row per customer_unique_id
--    - Handle multiple states per customer correctly
-- ============================================================

customer_agg as (

    select
        customer_unique_id,

        -- FIX: take state from most recent order (logical + consistent)
        max_by(customer_state, order_purchased_at) as customer_state,
		
        -- -------------------------
        -- ORDER BEHAVIOUR METRICS
        -- -------------------------
        count(order_id) as total_orders,                    -- how many times they purchased
        min(order_purchased_at) as first_order_at,          -- when they first became a customer
        max(order_purchased_at) as last_order_at,           -- most recent activity

        -- -------------------------
        -- VALUE METRICS
        -- -------------------------
        sum(order_gross_value) as lifetime_value,           -- total revenue from this customer
        avg(order_gross_value) as avg_order_value,          -- typical order size

        -- -------------------------
        -- DELIVERY EXPERIENCE
        -- These influence satisfaction + churn
        -- -------------------------
        avg(total_delivery_days) as avg_delivery_days,
        avg(delivery_delay_days) as avg_delivery_delay,
        sum(is_late_delivery) as late_delivery_count,
        round(sum(is_late_delivery)*100.0/count(order_id),2) as pct_orders_late,

        -- -------------------------
        -- CUSTOMER EXPERIENCE (REVIEWS)
        -- -------------------------
        avg(review_score) as avg_review_score,
        sum(is_low_satisfaction) as low_review_count,
        round(sum(is_low_satisfaction)*100.0/count(order_id),2) as pct_low_reviews,

        -- -------------------------
        -- RECENCY METRIC
        -- Critical for churn logic.
        -- Dataset ends on 2018-10-17.
        -- We measure how long since their last order.
        -- -------------------------
        datediff(to_date('2018-10-17'), max(order_purchased_at)) as days_since_last_order,

        -- -------------------------
        -- CHURN DEFINITION
        -- Business rule:
        --   If no order in last 180 days → churned
        -- -------------------------
        case
            when datediff(to_date('2018-10-17'), max(order_purchased_at)) > 180 then 1
            else 0
        end as is_churned

    from orders_with_unique_id
    -- CRITICAL FIX is group by customer unique id only rather than customer unique id and customer state :-
    group by customer_unique_id
)

select * from customer_agg;
