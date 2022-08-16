with source as (
    select *
    from {{ source('jshop','payments') }}
),
renamed as (
    select
        ID as payment_id,
        ORDER_ID as order_id,
        PAYMENT_METHOD as payment_method,
        AMOUNT as payment_amount
    from source
)
select * from renamed