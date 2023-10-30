with 
    source as (select * from {{ ref('src_tasty_bytes__city') }}),

    renamed as (
        select
            city_id,
            city as name,
            city_population as population,
            country_id
        from
            source
    )

select * from renamed


