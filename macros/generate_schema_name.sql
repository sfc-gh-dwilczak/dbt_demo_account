{% macro generate_schema_name(custom_schema_name=none, node=none) %}
    {% if target.name not in ('test', 'production') %}
        {{ return(target.schema) }}
    {% elif custom_schema_name is not none %}
        {{ return(custom_schema_name) }}
    {% endif %}
    {% set prefix = node.name.split('__')[0] %}
    {% set schema_split = prefix.split('_')[1:] %}
    {{ return(schema_split | join('_')) }}
{% endmacro %}
