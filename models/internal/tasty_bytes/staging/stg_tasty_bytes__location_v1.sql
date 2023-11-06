with
    source as (
        select *
        from {{ ref('snp_tasty_bytes__location_v1') }}
        {{ filter_snapshot_last_seen('location_id') }}
    ),

    renamed as (
        select
            location_id,
            placekey as place_key,
            location as location_name,
            region as local_region_name,
            city as city_name,
            iso_country_code as country_iso_code,
            country as country_name,
            dbt_scd_id,
            dbt_updated_at,
            dbt_valid_from,
            dbt_valid_to,
            dbt_valid_from as dwh_effective_from
        from
            source
    )

select * from renamed
