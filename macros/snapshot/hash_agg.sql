{% macro hash_agg(model) %}
    {{ return('hash_agg("' ~ (sorted_columns(model) | join('", "')) ~ '")') }}
{% endmacro %}
