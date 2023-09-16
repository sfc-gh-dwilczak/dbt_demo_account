<<<<<<< HEAD
{% macro generate_database_name(custom_database_name=none, node=none) %}
    {% set prefix = custom_database_name or '' %}
    {% if target.name == 'test' %}
        {% set prefix = 'dbt_test' %}
    {% else %}
        {% set prefix = 'dbt' %}
    {% endif %}
    {% if custom_database_name is not none %}
        {% set prefix = prefix ~ '_' ~ custom_database_name %}
    {% endif %}
    {% if target.name not in ('test', 'production') %}
        {{ return(target.database) }}
    {% elif node.name.startswith('stg') %}
        {% set db = 'staging' %}
    {% elif node.name.startswith('src') %}
        {% set db = 'sources' %}
    {% elif node.name.startswith('int') %}
        {% set db = 'intermediates' %}
    {% elif node.name.startswith('dim') %}
        {% set db = 'dimensions' %}
    {% elif node.name.startswith('fct') %}
        {% set db = 'facts' %}
    {% else %}
        {{ return(prefix) }}
    {% endif %}
    {{ return(prefix ~ '_' ~ db) }}
{% endmacro %}
=======
{% macro generate_database_name(custom_database_name=none, node=none) %}
    {% if target.name not in ('test', 'production') or custom_database_name is none %}
        {{ return(target.database) }}
    {% else %}
        {{ return(custom_database_name | trim) }}
    {% endif %}
{% endmacro %}


>>>>>>> fddaa3ea11c4464caf0943229412ec9fb5615c7c
