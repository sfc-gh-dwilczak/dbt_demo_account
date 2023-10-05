{% macro generate_alias_name(custom_alias_name=none, node=none) %}
    {% if target.name == 'production' %}
        {{ return(custom_alias_name or node.name.split('__')[-1]) }}
    {% else %}
        {{ return(custom_alias_name or node.name) }}
    {% endif %}
{% endmacro %}
