with 
    source as (select * from {{ ref('snp_google_sheets__linear_regression_v1') }}),

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
            filtered
    )

select * from renamed
