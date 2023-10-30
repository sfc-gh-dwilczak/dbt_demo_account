{% macro generate_schema_name(custom_schema_name, node) %}
    {% if node.name == 'metricflow_time_spine' %}
        {{ return('semantic_layer') }}
    {% elif custom_schema_name is not none %}
        {{ return(custom_schema_name) }}
    {% elif target.name == 'production' %}
        {{ return(node.name.split("__")[0].split("_")[1:] | join("_")) }}
    {% else %}
        {{ return(target.schema) }}
    {% endif %}
{% endmacro %}
