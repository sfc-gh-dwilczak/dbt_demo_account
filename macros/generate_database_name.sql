{% macro generate_database_name(custom_database_name=none, node=none) %}
    {% if target.name not in ('test', 'production') or custom_database_name is none %}
        {{ return(target.database) }}
    {% else %}
        {{ return(custom_database_name | trim) }}
    {% endif %}
{% endmacro %}


