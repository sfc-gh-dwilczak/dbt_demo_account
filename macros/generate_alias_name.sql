{% macro generate_alias_name(custom_alias_name, node) %}
    {% if node.version is none %}
        {% set suffix = '' %}
    {% else %}
        {% set suffix = '_v' ~ node.version %}
    {% endif %}
    {% if node.name == 'metricflow_time_spine' %}
        {{ return(node.name) }}
    {% elif custom_alias_name is not none %}
        {{ return(custom_alias_name ~ suffix) }}
    {% elif target.name == 'production' %}
        {{ return(
            (
                node.name.split("__")[0].split("_")[0])
                ~ "_"
                ~ (node.name.split("__")[1:] | join("__")
            )
            ~ suffix
        ) }}
    {% else %}
        {{ return(node.name ~ suffix) }}
    {% endif %}
{% endmacro %}
