{% snapshot snp_snowflake_usage__service_v1 %}
select
    {{ dbt_utils.generate_surrogate_key(
        ['service_type', 'entity_id', 'start_time']
    ) }} as service_usage_id,
    *
from
    {{ ref('src_snowflake_usage__service') }}
{% endsnapshot %}
