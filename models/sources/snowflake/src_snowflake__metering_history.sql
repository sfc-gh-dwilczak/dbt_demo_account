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

select * from renamed qualify count(*) over (partition by ) > 1

/*
{{ generate_semantic_layer(
    name='metering_history',
    references=this,
    time='start_time',
    dimensions=['name', 'service_type'],
    count='',
    sums=['credits_used', 'credits_used_compute', 'credits_used_cloud_services']
) }}
  - name: compute_used
    label: Compute Used
    type: derived
    type_params:
      expr: round(compute, 1)
      metrics:
        - name: credits_used_compute
          alias: compute
  - name: cloud_services_used
    label: Cloud Services Used
    type: derived
    type_params:
      expr: round(compute, 1)
      metrics:
        - name: credits_used_cloud_services
          alias: compute
*/