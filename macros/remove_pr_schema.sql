{% macro remove_pr_schema() %}
{%- set get_pr_schemas_sql -%}
    show schemas like 'ci_%'
{%- endset -%}

{%- set results = run_query(get_pr_schemas_sql) -%}

{%- if execute -%}
    {%- for schema in results -%}

        {%- set drop_schema_sql -%}
            drop schema if exists {{ target.database ~ schema['name'] }}
        {%- endset -%}
        
        {{ drop_schema_sql }};
        {# do run_query(drop_schema_sql) #}

    {%- endfor -%}
{%- endif -%}
{% endmacro %}