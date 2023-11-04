with
    source as (select * from {{ ref('snp_snowflake_usage__service_v1') }}),

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
            service_type,
            start_time,
            end_time,
            entity_id,
            name as service_name,
            credits_used_compute,
            credits_used_cloud_services,
            credits_used,
            "ROWS" as rows_clustered,
            bytes,
            files,
            budget_id,
            dbt_scd_id,
            dbt_updated_at,
            dbt_valid_from,
            dbt_valid_to,
            start_time as dwh_effective_from
        from
            filtered
    )

select * from renamed
