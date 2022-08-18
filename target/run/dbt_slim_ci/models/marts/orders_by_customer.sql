
  create or replace  view TROUZE_DB.JAFFLE_SHOP.orders_by_customer
  
   as (
    with c as (
    select * from TROUZE_DB.JAFFLE_SHOP.dim_customers
),
o as (
    select * from TROUZE_DB.JAFFLE_SHOP.fct_orders
)
select
    c.customer_id as customer_id,
    c.first_name as first_name,
    c.last_initial as last_initial,
    max(o.order_date) as max_order_date,
    min(o.order_date) as min_order_date
    -- count(o.order_date) as num_orders
from c
left join o on o.customer_id = c.customer_id
where o.customer_id is not null
group by c.customer_id, c.first_name, c.last_initial
  );
