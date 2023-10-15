with 
    source as (select * from {{ source('tasty_bytes', 'city') }}),

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


