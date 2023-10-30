{% snapshot scd_snowflake__stage_storage_usage_history %}
select * from {{ ref('src_snowflake__stage_storage_usage_history') }}
{% endsnapshot %}
