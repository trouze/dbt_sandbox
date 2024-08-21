with source as (
    select *
    from {{ source('jaffle_shop','raw_items') }}
),
renamed as (
    select
        ID as item_id,
        ORDER_ID as order_id,
        SKU as product_id,
        LOAD_DTS as load_dts,
        current_timestamp() as last_model_run,
        'hello_world' as test_change
    from source
)
select * from renamed