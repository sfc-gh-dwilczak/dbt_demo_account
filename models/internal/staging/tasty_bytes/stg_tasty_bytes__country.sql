with 

source as (

    select * from {{ source('tasty_bytes', 'city') }}

),

renamed as (

    select distinct
        country_id,
        country as name,
        iso_currency,
        iso_country as iso_country_code

    from source

)

select * from renamed
