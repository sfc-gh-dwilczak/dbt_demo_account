with 

source as (

    select * from {{ ref('src_snowflake__metering_history') }}

),

renamed as (

    select
        service_type,
        start_time,
        end_time,
        entity_id,
        name,
        credits_used_compute,
        credits_used_cloud_services,
        credits_used,
        "ROWS" as rows_clustered,
        bytes,
        files,
        budget_id

    from source

)

select * from renamed
