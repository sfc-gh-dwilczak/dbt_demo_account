{% macro filter_snapshot_last_seen(key) %}
qualify rank() over (partition by {{ key }} order by dbt_valid_from desc) = 1
{% endmacro %}
