{% macro sorted_select_star(model, quote=true) %}
    {{ return(sorted_columns(model, quote) | join(', ')) }}
{% endmacro %}
