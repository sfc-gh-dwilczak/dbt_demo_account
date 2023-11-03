select * from {{ source('snowflake', 'stage_storage_usage_history') }}
