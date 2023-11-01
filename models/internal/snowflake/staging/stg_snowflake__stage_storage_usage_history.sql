with
    source as (
        select * from {{ ref('scd_snowflake__stage_storage_usage_history_v1') }}
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
            usage_date,
            average_stage_bytes,
            dbt_scd_id,
            dbt_updated_at,
            dbt_valid_from,
            dbt_valid_to,
            usage_date::timestamp_ntz as dwh_effective_from
        from
            filtered
    )

select * from renamed
