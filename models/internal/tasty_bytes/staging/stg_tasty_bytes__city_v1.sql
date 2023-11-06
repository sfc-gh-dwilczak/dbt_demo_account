with 
    source as (
        select *
        from {{ ref('snp_tasty_bytes__city_v1') }}
        {{ filter_snapshot_last_seen('city_id') }}
    ),

    renamed as (
        select
            city_id,
            city as city_name,
            city_population,
            country_id,
            country as country_name,
            iso_country as country_iso_code,
            iso_currency as currency_iso_code,
            dbt_scd_id,
            dbt_updated_at,
            dbt_valid_from,
            dbt_valid_to,
            dbt_valid_from as dwh_effective_from
        from
            source
    )

select * from renamed
