{% snapshot snp_snowflake_usage__warehouse_v1 %}
select
    {{ dbt_utils.generate_surrogate_key(
        ['warehouse_id', 'start_time']
    ) }} as warehouse_usage_id,
    *
from
    {{ ref('src_snowflake_usage__warehouse') }}
{% endsnapshot %}
