{% snapshot snp_snowflake_usage__storage_v1 %}
select
    *
from
    {{ ref('src_snowflake_usage__storage') }}
{% endsnapshot %}
