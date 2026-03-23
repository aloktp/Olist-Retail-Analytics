with source as (
    select * 
    from {{ source('bronze', 'order_reviews') }}
),
cleaned as (
    select
        review_id,
        order_id,

        -- try_cast instead of cast: returns NULL for bad values (e.g. timestamps
        -- accidentally stored in review_score) instead of crashing the pipeline
        try_cast(review_score as int) as review_score,

        case 
            when try_cast(review_score as int) <= 2 then 1 
            else 0 
        end as is_low_satisfaction,

        -- Olist reviews format: 'dd/MM/yyyy HH:mm' e.g. '01/04/2018 00:27'
        -- try_to_timestamp instead of to_timestamp: returns NULL for bad values
        -- 'dd/MM/yyyy HH:mm' not 'd/M/yyyy H:mm' — single d/M fails on dates
        -- like '15/04/2018' where day and month are two digits
        try_to_timestamp(review_creation_date, 'dd/MM/yyyy HH:mm') as review_created_at

    from source
    where order_id is not null
      and review_score is not null
)
select * from cleaned;