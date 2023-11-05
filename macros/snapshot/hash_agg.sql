{% macro hash_agg(model) %}
hash_agg({{ sorted_select_star(model) }})
{% endmacro %}
