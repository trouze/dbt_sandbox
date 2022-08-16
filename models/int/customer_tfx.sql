select
    ID,
    upper(FIRST_NAME) as FIRST_NAME,
    SIGNUP_DATE
from {{ref('customers_latest')}}