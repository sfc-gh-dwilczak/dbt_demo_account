{% macro generate_schema_name(custom_schema_name, node) %}
    {% if target.name not in ('test', 'production') or custom_schema_name is none %}
        {{ return(target.schema) }}
    {% else %}
        {{ return(custom_schema_name | trim) }}
    {% endif %}
{% endmacro %}