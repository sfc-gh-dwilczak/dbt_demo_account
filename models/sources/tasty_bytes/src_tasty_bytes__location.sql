with 

source as (

    select * from {{ source('tasty_bytes', 'location') }}

),

renamed as (

    select
        location_id,
        placekey,
        location,
        city,
        region,
        iso_country_code,
        country

    from source

)

select * from renamed
