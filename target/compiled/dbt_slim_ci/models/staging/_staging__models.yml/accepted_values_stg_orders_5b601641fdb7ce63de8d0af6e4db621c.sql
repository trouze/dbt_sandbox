
    
    

with all_values as (

    select
        status as value_field,
        count(*) as n_records

    from TROUZE_DB.JAFFLE_SHOP.stg_orders
    group by status

)

select *
from all_values
where value_field not in (
    'returned','completed','return_pending','shipped','placed'
)


