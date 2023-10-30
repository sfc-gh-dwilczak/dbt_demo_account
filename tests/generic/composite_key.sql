{% test composite_key(model, columns, allow_nulls=[]) %}
select
    {{ columns | join(",\n    ") }}
from
    {{ model }}
group by
    {{ columns | join(",\n    ") }}
having
    count(*) > 1
    {%- if not allow_nulls -%}
    {%- for column in columns if column not in allow_nulls %}
    or {{ column }} is null
    {%- endfor -%}
    {%- endif %}
{% endtest %}


