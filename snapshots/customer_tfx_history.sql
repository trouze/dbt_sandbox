{% snapshot customer_tfx_history %}

{{
    config(
      target_database='TROUZE_DB',
      target_schema='JAFFLE_SHOP',
      unique_key='ID',

      strategy='timestamp',
      updated_at='SIGNUP_DATE',
    )
}}

select * from {{ source('jshop','customers') }}

{% endsnapshot %}