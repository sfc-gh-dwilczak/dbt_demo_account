select * from {{ source('snowflake_usage', 'storage') }}
