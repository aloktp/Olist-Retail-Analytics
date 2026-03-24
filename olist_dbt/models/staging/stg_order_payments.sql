-- ============================================================
-- PURPOSE:
-- Clean order payments data
-- Each row represents one payment event for an order
-- Primary key (logical): (order_id, payment_sequential)
-- ============================================================

with source as (

    select * 
    from {{ source('bronze', 'order_payments') }}

),

cleaned as (

    select
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,

        cast(payment_value as double) as payment_value

    from source

    -- Remove null primary keys
    where order_id is not null
      and payment_sequential is not null

)

select * from cleaned