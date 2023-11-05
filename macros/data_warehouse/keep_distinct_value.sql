{% macro keep_distinct_value(value, indent=3) %}
{% set sql -%}
case
    when min({{ value }}) = max({{ value }})
    then min({{ value }})
end
{%- endset %}
{{ return(sql | replace('\n', '\n' ~ ('    ' * indent))) }}
{% endmacro %}
