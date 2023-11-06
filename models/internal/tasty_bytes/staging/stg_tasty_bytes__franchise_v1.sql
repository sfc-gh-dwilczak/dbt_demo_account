with
    source as (
        select *
        from {{ ref('snp_tasty_bytes__franchise_v1') }}
        {{ filter_snapshot_last_seen('franchise_id, city') }}
    ),

    renamed as (
        select
            franchise_id,
            first_name,
            last_name,
            city as city_name,
            country as country_name,
            e_mail as email,
            phone_number,
            dbt_scd_id,
            dbt_updated_at,
            dbt_valid_from,
            dbt_valid_to,
            dbt_valid_from as dwh_effective_from
        from
            source
    )

select * from renamed
