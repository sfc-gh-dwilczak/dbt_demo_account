with
    source as (select * from {{ ref('src_tasty_bytes__customer') }}),
    city as (select * from {{ ref('stg_tasty_bytes__city') }}),

    renamed as (
        select
            customer_id,
            first_name,
            last_name,
            city.city_id,
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
        from
            source
        left join
            city
                on city.name = source.city
    )

select * from renamed
