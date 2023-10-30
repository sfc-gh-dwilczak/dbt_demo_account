{% snapshot scd_snowflake__metering_history %}
select
    {{ dbt_utils.generate_surrogate_key(
        ['service_type', 'entity_id', 'start_time']
    ) }} as metering_history_id,
    0 as dbt_check_col,
    *
from
    {{ ref('src_snowflake__metering_history') }}
{% endsnapshot %}
