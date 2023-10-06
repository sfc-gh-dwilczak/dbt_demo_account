with 

source as (

    select * from {{ source('snowflake', 'metering_history') }}

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

/*
{{ generate_semantic_layer(
    name='metering_history',
    references=this,
    time='start_time',
    dimensions=['name', 'service_type'],
    count='',
    sums=['credits_used', 'credits_used_compute', 'credits_used_cloud_services']
) }}
  
*/