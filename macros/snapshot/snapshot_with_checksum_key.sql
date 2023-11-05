{% macro snapshot_with_checksum_key(model, keys) %}
select
    {{ sorted_select_star(model) }},
    {{ dbt_utils.generate_surrogate_key(
        keys + ['hash(' ~ sorted_select_star(model) ~ ')']
    ) }} as dbt_scd_uk
from
    {{ model }}
{% endmacro %}
