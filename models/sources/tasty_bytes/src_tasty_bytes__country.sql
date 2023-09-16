with 

source as (

    select * from {{ source('tasty_bytes', 'city') }}

),

renamed as (

    select distinct
        country_id,
        country,
        iso_currency,
        iso_country

    from source

)

select * from renamed
