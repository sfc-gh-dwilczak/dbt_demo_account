select * from {{ source('snowflake', 'metering_history') }}
