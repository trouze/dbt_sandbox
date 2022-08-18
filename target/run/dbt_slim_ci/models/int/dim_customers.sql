
  create or replace  view TROUZE_DB.JAFFLE_SHOP.dim_customers
  
   as (
    select
    customer_id,
    upper(first_name) as first_name,
    trim('.',last_name) as last_initial,
    signup_date
from TROUZE_DB.JAFFLE_SHOP.stg_customers
  );
