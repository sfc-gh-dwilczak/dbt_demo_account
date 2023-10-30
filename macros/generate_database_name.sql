{% macro generate_database_name(custom_database_name, node) %}
    {% if target.name == 'production' %}
        {{ return(custom_database_name) }}
    {% else %}
        {{ return(target.database) }}
    {% endif %}
{% endmacro %}
