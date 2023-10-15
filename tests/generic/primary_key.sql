{% macro primary_key(model, column_name, allow_nulls=false) %}
select
    {{ column_name }}
from
    {{ model }}
group by
    {{ column_name }}
having
    count(*) > 1 {%- if not allow_nulls %} or {{ column_name }} is null {%- endif %}
{% endmacro %}
