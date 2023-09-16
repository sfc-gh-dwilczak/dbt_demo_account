with 

source as (

    select * from {{ source('tasty_bytes', 'customer') }}

),

renamed as (

    select
        customer_id,
        first_name,
        last_name,
        city as city_name,
        case country
            when 'United Kingdom'
            then 'England'
            else country
        end as country_name,
        postal_code,
        preferred_language,
        gender,
        favourite_brand,
        marital_status,
        children_count,
        sign_up_date,
        birthday_date,
        e_mail as email,
        phone_number

    from source

)

select * from renamed
