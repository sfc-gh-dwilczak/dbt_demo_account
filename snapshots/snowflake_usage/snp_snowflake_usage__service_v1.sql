{% snapshot snp_snowflake_usage__service_v1 %}
select
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['service_type', 'entity_id', 'start_time', 'hash(*)']
    ) }} as dbt_unique_key
from
    {{ ref('src_snowflake_usage__service') }}
{% endsnapshot %}
