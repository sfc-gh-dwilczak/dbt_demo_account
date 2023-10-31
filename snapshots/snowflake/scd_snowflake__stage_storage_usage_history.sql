{% snapshot scd_snowflake__stage_storage_usage_history %}
select 0 as dbt_check_col, * from {{ ref('src_snowflake__stage_storage_usage_history') }}
{% endsnapshot %}
