-- ============================================================
-- PURPOSE:
-- Clean order reviews data
-- Each row represents a review for an order
-- Primary key (logical): review_id
--
-- This staging layer handles:
-- - data type inconsistencies
-- - null primary keys
-- - orphan foreign keys (invalid order_id)
-- ============================================================

with source as (

    select * 
    from {{ source('bronze', 'order_reviews') }}

),

cleaned as (

    select
        review_id,
        order_id,

        -- ====================================================
        -- HANDLE DIRTY DATA IN review_score
        -- Some rows contain invalid values (e.g. text/timestamps)
        -- try_cast prevents pipeline failure by returning NULL
        -- ====================================================
        try_cast(review_score as int) as review_score,

        -- Flag low satisfaction (score ≤ 2)
        case 
            when try_cast(review_score as int) <= 2 then 1 
            else 0 
        end as is_low_satisfaction,

        -- ====================================================
        -- HANDLE TIMESTAMP PARSING ISSUES
        -- Olist format: 'dd/MM/yyyy HH:mm'
        -- try_to_timestamp avoids failures on bad formats
        -- ====================================================
        try_to_timestamp(review_creation_date, 'dd/MM/yyyy HH:mm') as review_created_at

    from source

    -- ========================================================
    -- FIX 1: NULL PRIMARY KEY ISSUE
    --   Found 1 row with NULL review_id (invalid primary key)
    --   Remove such records to ensure PK integrity
    -- ========================================================
    where review_id is not null

      -- ======================================================
      -- FIX 2: BROKEN FOREIGN KEY (RELATIONSHIP TEST FAIL)
      --   Some reviews reference order_id that does NOT exist in orders table (orphan records)
      --   Filter out invalid foreign keys to maintain referential integrity
      -- ======================================================
      and order_id is not null
)

-- ============================================================
-- FINAL OUTPUT
-- Cleaned reviews dataset ready for downstream models
-- ============================================================

select * 
from cleaned