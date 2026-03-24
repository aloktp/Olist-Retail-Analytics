-- ============================================================
-- PURPOSE:
-- Clean order items data
-- Each row represents one item within an order
-- Primary key (logical): (order_id, order_item_id)
-- ============================================================

with source as (

    select * 
    from {{ source('bronze', 'order_items') }}

),

cleaned as (

    select
        order_id,
        order_item_id,
        product_id,
        seller_id,

        -- VALUE METRICS
        cast(price as double) as item_price,
        cast(freight_value as double) as freight_value,
        cast(price as double) + cast(freight_value as double) as item_total_value,

        -- TIMESTAMP
        to_timestamp(shipping_limit_date, 'd/M/yyyy H:mm') as shipping_limit_at

    from source

    -- Remove null primary keys
    where order_id is not null
      and order_item_id is not null

)

select * from cleaned