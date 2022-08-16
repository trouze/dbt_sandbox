select
    order_id,
    customer_id,
    date(order_date) as order_date,
    order_status
from {{ ref('stg_orders') }}