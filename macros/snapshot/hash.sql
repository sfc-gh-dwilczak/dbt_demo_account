{% macro hash(model) %}
hash({{ sorted_select_star(model) }})
{% endmacro %}
