select
    customer_id,
    upper(first_name) as first_name,
    left(last_name,1) as last_initial,
    signup_date
from {{ ref('stg_customers') }}