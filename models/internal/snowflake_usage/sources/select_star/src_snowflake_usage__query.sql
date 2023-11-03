select * from {{ source('snowflake_usage', 'query') }}
