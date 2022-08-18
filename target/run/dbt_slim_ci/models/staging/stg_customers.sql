

      create or replace transient table TROUZE_DB.JAFFLE_SHOP.stg_customers  as
      (with source as (
    select *
    from TROUZE_DB.JAFFLE_SHOP.customers
),
renamed as (
    select
        ID as customer_id,
        FIRST_NAME as first_name,
        LAST_NAME as last_name,
        SIGNUP_DATE as signup_date
    from source
)
select * from renamed
      );
    