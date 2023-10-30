with 

source as (

    select * from {{ ref('src_tasty_bytes__city') }}

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
