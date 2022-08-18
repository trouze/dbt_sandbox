select
    order_id,
    customer_id,
    date(order_date) as order_date,
    order_status
from TROUZE_DB.JAFFLE_SHOP.stg_orders