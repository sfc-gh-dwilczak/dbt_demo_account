{% macro snapshot_with_checksum_key(model, keys, unique_key_alias) %}
select
    {{ sorted_select_star(model) }},
    {{ dbt_utils.generate_surrogate_key(
        keys + [hash(model)]
    ) }} as {{ unique_key_alias }}
from
    {{ model }}
{% endmacro %}
