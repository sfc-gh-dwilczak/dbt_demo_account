{% snapshot snp_snowflake_usage__warehouse_v1 %}
select
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['warehouse_id', 'start_time', 'hash(*)']
    ) }} as dbt_unique_key
from
    {{ ref('src_snowflake_usage__warehouse') }}
{% endsnapshot %}
