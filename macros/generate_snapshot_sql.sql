{% macro generate_snapshot_sql(model, keys) %}
select
    *,
    {{ dbt_utils.generate_surrogate_key(
        keys + ['hash(*)']
    ) }} as dbt_unique_key
from (
    select
        {{ sorted_columns(model) | join(',\n        ') }}
    from
        {{ model }}
)
{% endmacro %}
