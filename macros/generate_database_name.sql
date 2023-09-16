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
    {% elif node.name.startswith('seed') %}
        {% set db = 'seeds' %}
    {% elif node.name.startswith('snapshots') %}
        {% set db = 'snapshots' %}
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
