with 
    source as (select * from {{ ref('snp_tasty_bytes__city_v1') }}),

    filtered as (
        select
            *
        from
            source
        qualify
            row_number() over (
                partition by
                    city_id
                order by
                    dbt_valid_from desc
            ) = 1
    ),

    renamed as (
        select
            city_id,
            city as city_name,
            city_population,
            country_id,
            country as country_name,
            iso_country as iso_country_code,
            iso_currency as iso_currency_code,
            dbt_scd_id,
            dbt_updated_at,
            dbt_valid_from,
            dbt_valid_to,
            dbt_valid_from as dwh_effective_from
        from
            filtered
    )

select * from renamed
