with c as (
    select * from {{ref('customers_latest')}}
),
o as (
    select * from {{ref('orders_latest')}}
)
select
    c.ID as ID,
    c.FIRST_NAME as FIRST_NAME,
    c.LAST_NAME as LAST_NAME,
    o.ORDER_DATE as ORDER_DATE,
    o.STATUS as "STATUS"
from c
left join o on o.USER_ID = c.ID
where o.USER_ID is not null