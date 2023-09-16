with 

source as (

    select * from {{ source('tasty_bytes', 'franchise') }}

),

renamed as (

    select distinct
        franchise_id,
        first_name,
        last_name,
        city as city_name,
        country as country_name,
        e_mail as email,
        phone_number

    from source

)

select * from renamed
