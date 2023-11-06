select * from {{ source('snowflake_usage', 'service') }}
