{% test id_too_high(model, column_name) %}

    {{ config(store_failures = true) }}

    select {{ column_name }}
    from {{ model }}
    where {{ column_name }} > 80

{% endtest %}