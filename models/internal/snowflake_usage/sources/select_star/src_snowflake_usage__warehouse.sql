select * from {{ source('snowflake_usage', 'warehouse') }}
