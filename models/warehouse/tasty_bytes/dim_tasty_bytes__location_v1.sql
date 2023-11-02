with
    tasty_bytes as (select * from {{ ref('stg_tasty_bytes__city', v=1) }}),

    city_ids as (
        select
            city_id,
            city_name,
            city_population,
            country_id,
            country_name,
            iso_country_code,
            iso_currency_code,
            'tasty_bytes' as dwh_source,
            1 as dwh_version,
            'city_id' as dwh_granularity,
            {{ dbt_utils.generate_surrogate_key(
                ['dwh_source', 'dwh_version', 'dwh_granularity', 'city_id']
            ) }} as dwh_location_id
        from
            tasty_bytes
    ),

    city_names as (
        select
            city_id,
            city_name,
            city_population,
            country_id,
            country_name,
            iso_country_code,
            iso_currency_code,
            'tasty_bytes' as dwh_source,
            1 as dwh_version,
            'city_name' as dwh_granularity,
            {{ dbt_utils.generate_surrogate_key(
                ['dwh_source', 'dwh_version', 'dwh_granularity', 'city_name']
            ) }} as dwh_location_id
        from
            tasty_bytes
    ),

    country_ids as (
        select
            null as city_id,
            null as city_name,
            null as city_population,
            country_id,
            any_value(country_name) as country_name,
            any_value(iso_country_code) as iso_country_code,
            any_value(iso_currency_code) as iso_currency_code,
            'tasty_bytes' as dwh_source,
            1 as dwh_version,
            'country_id' as dwh_granularity,
            {{ dbt_utils.generate_surrogate_key(
                ['dwh_source', 'dwh_version', 'dwh_granularity', 'country_id']
            ) }} as dwh_location_id
        from
            tasty_bytes
        group by
            country_id
    ),

    country_names as (
        select
            null as city_id,
            null as city_name,
            null as city_population,
            any_value(country_id) as country_id,
            country_name,
            any_value(iso_country_code) as iso_country_code,
            any_value(iso_currency_code) as iso_currency_code,
            'tasty_bytes' as dwh_source,
            1 as dwh_version,
            'country_name' as dwh_granularity,
            {{ dbt_utils.generate_surrogate_key(
                ['dwh_source', 'dwh_version', 'dwh_granularity', 'country_name']
            ) }} as dwh_location_id
        from
            tasty_bytes
        group by
            country_name
    ),

    iso_country_codes as (
        select
            null as city_id,
            null as city_name,
            null as city_population,
            any_value(country_id) as country_id,
            any_value(country_name) as country_name,
            iso_country_code,
            any_value(iso_currency_code) as iso_currency_code,
            'tasty_bytes' as dwh_source,
            1 as dwh_version,
            'iso_country_code' as dwh_granularity,
            {{ dbt_utils.generate_surrogate_key(
                ['dwh_source', 'dwh_version', 'dwh_granularity', 'iso_country_code']
            ) }} as dwh_location_id
        from
            tasty_bytes
        group by
            iso_country_code
    ),

    merged as (
        select * from city_ids
        union all
        select * from city_names
        union all
        select * from country_ids
        union all
        select * from country_names
        union all
        select * from iso_country_codes
    ),

    recolumned as (
        select
            dwh_location_id,
            dwh_source,
            dwh_version,
            dwh_granularity,
            * exclude (
                dwh_location_id,
                dwh_source,
                dwh_version,
                dwh_granularity
            )
        from
            merged
    )

select * from recolumned
