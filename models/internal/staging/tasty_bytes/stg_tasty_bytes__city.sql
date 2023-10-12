with 

source as (

    select * from {{ source('tasty_bytes', 'city') }}

),

renamed as (

    select
        city_id,
        city as city_name,
        city_population,
        country_id

    from source

)

select * from renamed
