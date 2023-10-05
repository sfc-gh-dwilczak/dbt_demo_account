with 

source as (

    select * from {{ source('snowflake', 'warehouse_metering_history') }}

),

renamed as (

    select
        start_time,
        end_time,
        warehouse_id,
        warehouse_name,
        credits_used,
        credits_used_compute,
        credits_used_cloud_services

    from source

)

select * from renamed
