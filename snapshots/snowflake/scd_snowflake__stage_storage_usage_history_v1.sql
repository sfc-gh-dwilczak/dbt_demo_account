{% snapshot scd_snowflake__stage_storage_usage_history_v1 %}
select
    0 as dv_hashdiff,
    *
from
    {{ ref('src_snowflake__stage_storage_usage_history') }}
{% endsnapshot %}
