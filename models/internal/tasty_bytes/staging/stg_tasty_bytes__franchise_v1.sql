with
    source as (select * from {{ ref('snp_tasty_bytes__franchise_v1') }}),

    filtered as (
        select
            *
        from
            source
        qualify
            rank() over (
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
