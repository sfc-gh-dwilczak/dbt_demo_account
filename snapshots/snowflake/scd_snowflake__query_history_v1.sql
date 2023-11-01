{% snapshot scd_snowflake__query_history_v1 %}
select
    0 as dv_hashdiff,
    *
from
    {{ ref('src_snowflake__query_history') }}
{% endsnapshot %}
