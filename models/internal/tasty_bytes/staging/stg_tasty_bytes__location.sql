with 

source as (

    select * from {{ ref('src_tasty_bytes__location') }}

),

renamed as (

    select
        location_id,
        placekey as place_key,
        location as location_name,
        city as city_name,
        region as region_name,
        iso_country_code,
        country as country_name

    from source

)

select * from renamed
