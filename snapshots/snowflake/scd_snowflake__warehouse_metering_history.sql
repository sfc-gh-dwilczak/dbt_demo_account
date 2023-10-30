{% snapshot scd_snowflake__warehouse_metering_history %}
select
    {{ dbt_utils.generate_surrogate_key(
        ['warehouse_id', 'start_time']
    ) }} as warehouse_metering_history_id,
    *
from
    {{ ref('src_snowflake__warehouse_metering_history') }}
{% endsnapshot %}
