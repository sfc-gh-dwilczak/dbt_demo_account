with
    source as (select * from {{ ref('snp_tasty_bytes__franchise_v1') }}),

    filtered as (
        select
            *
        from
            source
        qualify
            row_number() over (
                partition by
                    franchise_id,
                    city
                order by
                    dbt_valid_from desc
            ) = 1
    ),

    renamed as (
        select
            franchise_id,
            first_name,
            last_name,
            city as city_name,
            country as country_name,
            e_mail as email,
            phone_number
        from
            filtered
    )

select * from renamed
