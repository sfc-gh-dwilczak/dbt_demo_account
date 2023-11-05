{% test unique_combination_with_salt(model, combination_of_columns) %}
select distinct
    {{ combination_of_columns | join(',\n    ') }}
from
    {{ model }}
group by
    {{ combination_of_columns | join(',\n    ') }},
    hash({{ sorted_select_star(model) }})
having
    count(*) > 1
{% endtest %}
