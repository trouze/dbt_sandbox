{% test pct_null(model, column_name) %}

    select (COUNT(*) - COUNT({{ column_name }})) * 100.0 / COUNT(*) as dq_metric from {{ model }}

{% endtest %}