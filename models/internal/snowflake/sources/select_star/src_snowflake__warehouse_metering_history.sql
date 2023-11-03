select * from {{ source('snowflake', 'warehouse_metering_history') }}
