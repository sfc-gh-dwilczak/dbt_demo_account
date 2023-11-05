{% macro generate_database_name(custom_database_name, node) %}
    {% if target.name == 'production' and custom_database_name is not none %}
        {{ return(custom_database_name) }}
    {% else %}
        {{ return(target.database) }}
    {% endif %}
{% endmacro %}
