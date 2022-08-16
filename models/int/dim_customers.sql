select
    customer_id,
    upper(first_name) as first_name,
    trim('.',last_name) as last_initial,
    signup_date
from {{ ref('stg_customers') }}