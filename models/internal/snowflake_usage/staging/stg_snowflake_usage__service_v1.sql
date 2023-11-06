with
    source as (
        select *
        from {{ ref('snp_snowflake_usage__service_v1') }}
        {{ filter_snapshot_current() }}
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
            dbt_scd_uk as dbt_unique_key,
            dbt_scd_id,
            dbt_updated_at,
            dbt_valid_from,
            dbt_valid_to,
            start_time as dwh_effective_from
        from
            source
    )

select * from renamed
