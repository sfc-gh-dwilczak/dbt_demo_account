{% macro generate_alias_name(custom_alias_name, node) %}
    {% if custom_alias_name is not none %}
        {{ return(custom_alias_name) }}
    {% elif target.name == 'production' %}
        {{ return((node.name.split("__")[0].split("_")[0]) ~ "_" ~ (node.name.split("__")[1:] | join("__"))) }}
    {% else %}
        {{ return(node.name) }}
    {% endif %}
{% endmacro %}