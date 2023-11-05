with
    source as (
        select *
        from {{ ref('snp_snowflake_usage__storage_v1') }}
        {{ filter_snapshot_current() }}
    ),

    renamed as (
        select
            usage_date,
            average_stage_bytes,
            dbt_scd_uk as dbt_unique_key,
            dbt_scd_id,
            dbt_updated_at,
            dbt_valid_from,
            dbt_valid_to,
            usage_date::timestamp_ntz as dwh_effective_from
        from
            source
    )

select * from renamed
