select * from {{ source('snowflake', 'query_history') }}
