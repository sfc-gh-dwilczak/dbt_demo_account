with
    source as (select * from {{ ref('snp_tasty_bytes__customer_v1') }}),

    filtered as (
        select
            *
        from
            source
        qualify
            rank() over (
                partition by
                    customer_id
                order by
                    dbt_valid_from desc
            ) = 1
    ),

    renamed as (
        select
            customer_id,
            first_name,
            last_name,
            city as city_name,
            country as country_name,
            postal_code,
            preferred_language,
            gender,
            favourite_brand,
            marital_status,
            children_count,
            sign_up_date,
            birthday_date,
            e_mail as email,
            phone_number,
            dbt_scd_id,
            dbt_updated_at,
            dbt_valid_from,
            dbt_valid_to,
            dbt_valid_from as dwh_effective_from
        from
            filtered
    )

select * from renamed
