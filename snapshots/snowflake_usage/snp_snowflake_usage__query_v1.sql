{% snapshot snp_snowflake_usage__query_v1 %}
select
    *
from
    {{ ref('src_snowflake_usage__query') }}
{% endsnapshot %}
