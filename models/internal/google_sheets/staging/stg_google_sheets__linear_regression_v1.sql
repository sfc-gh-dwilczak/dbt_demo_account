with 
    source as (
        select *
        from {{ ref('snp_google_sheets__linear_regression_v1') }}
        {{ filter_snapshot_current() }}
    ),

    renamed as (
        select
            _row,
            x,
            y,
            _fivetran_synced,
            dbt_scd_id,
            dbt_updated_at,
            dbt_valid_from,
            dbt_valid_to,
            _fivetran_synced as dwh_effective_from
        from
            source
    )

select * from renamed
