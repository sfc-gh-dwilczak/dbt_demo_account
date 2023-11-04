{% snapshot snp_snowflake_usage__query_v1 %}
select
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['query_id', 'hash(*)']
    ) }} as dbt_unique_key
from
    {{ ref('src_snowflake_usage__query') }}
{% endsnapshot %}
