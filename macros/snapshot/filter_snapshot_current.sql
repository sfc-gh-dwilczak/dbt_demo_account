{% macro filter_snapshot_current() %}
where dbt_valid_to is null
{% endmacro %}
