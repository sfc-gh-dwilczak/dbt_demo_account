{% test unique_with_salt(model, column_name) %}
select distinct
    {{ column_name }}
from
    {{ model }}
group by
    {{ column_name }}, hash({{ sorted_select_star(model) }})
having
    count(*) > 1
{% endtest %}
