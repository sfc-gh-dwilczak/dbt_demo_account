with
    source as (
        select * from {{ ref('scd_snowflake__warehouse_metering_history_v1') }}
    ),

    filtered as (
        select
            *
        from
            source
        where
            dbt_valid_to is null
    ),

    renamed as (
        select
            start_time,
            end_time,
            warehouse_id,
            warehouse_name,
            credits_used,
            credits_used_compute,
            credits_used_cloud_services,
            dbt_scd_id,
            dbt_updated_at,
            dbt_valid_from,
            dbt_valid_to,
            start_time as dwh_effective_from
        from
            filtered
    )

select * from renamed
