{% macro distinct_values(model, limit=10) %}
select
    *
from (
{% for column in get_columns_in_relation(model) %}
    select
        *
    from (
        select
            '{{ column.name }}' as column_name,
            {{ column.name }}::varchar as value,
            count(*) as repeated_values,
            count(*) over () as distinct_values
        from
            {{ model }}
        group by
            {{ column.name }}
        order by
            repeated_values desc
        limit
            {{ limit }}
        )
{% if not loop.last %}
    union all
{% endif %}
{% endfor %}
)
{% endmacro %}