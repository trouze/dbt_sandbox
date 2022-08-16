{% snapshot orders_history %}

{{
    config(
      target_database='TROUZE_DB',
      target_schema='JAFFLE_SHOP',
      unique_key='ID',

      strategy='timestamp',
      updated_at='ORDER_DATE',
    )
}}

select * from {{ source('jshop', 'ORDERS') }}

{% endsnapshot %}