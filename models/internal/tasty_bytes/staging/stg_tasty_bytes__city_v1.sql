with 
    source as (select * from {{ ref('scd_tasty_bytes__city_v1') }}),

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
            city_population as population,
            country_id,
            country as country_name,
            iso_currency,
            iso_country as iso_country_code,
            dbt_scd_id,
            dbt_updated_at,
            dbt_valid_from,
            dbt_valid_to,
            dbt_valid_from as dwh_effective_from
        from
            filtered
    )

select * from renamed
