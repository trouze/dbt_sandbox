select
    ID,
    FIRST_NAME,
    LAST_NAME,
    SIGNUP_DATE
from {{ source('customers') }}