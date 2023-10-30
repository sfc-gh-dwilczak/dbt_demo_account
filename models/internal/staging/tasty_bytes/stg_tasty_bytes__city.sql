with 
    source as (select * from {{ ref('src_tasty_bytes__city') }}),

    renamed as (
        select
            city_id,
            city as name,
            city_population as population,
            country_id,
            country,
            iso_currency,
            iso_country as iso_country_code
        from
            source
    )

select * from renamed
