with
    truck as (select * from {{ ref('stg_tasty_bytes__truck', v=1) }}),

    truck_ids as (
        select
            truck_id,
            menu_type_id,
            {{ dbt_utils.generate_surrogate_key(
                ["'tasty_bytes'", "1", "'city_name'", "primary_city"]
            ) }} as dwh_location_id,
            primary_city,
            country_region_name,
            country_region_iso_code,
            country_name,
            country_iso_code,
            franchise_flag,
            truck_year,
            truck_make,
            truck_model,
            ev_flag,
            {{ dbt_utils.generate_surrogate_key(
                ["'tasty_bytes'", "1", "'franchise_id'", "franchise_id"]
            ) }} as dwh_franchise_id,
            truck_opening_date,
            'tasty_bytes' as dwh_source,
            1 as dwh_version,
            'truck_id' as dwh_granularity,
            {{ dbt_utils.generate_surrogate_key(
                ['dwh_source', 'dwh_version', 'dwh_granularity', 'truck_id']
            ) }} as dwh_truck_id
        from
            truck
    ),

    city_franchises as (
        select
            {{ keep_distinct_value('truck_id') }} as truck_id,
            {{ keep_distinct_value('menu_type_id') }} as menu_type_id,
            {{ dbt_utils.generate_surrogate_key(
                ["'tasty_bytes'", "1", "'city_name'", "primary_city"]
            ) }} as dwh_location_id,
            primary_city,
            any_value(country_region_name) as country_region_name,
            any_value(country_region_iso_code) as country_region_iso_code,
            any_value(country_name) as country_name,
            any_value(country_iso_code) as country_iso_code,
            {%- for column in [
                'franchise_flag',
                'truck_year',
                'truck_make',
                'truck_model',
                'ev_flag'
            ] %}
            {{ keep_distinct_value(column) }} as {{ column }},
            {%- endfor %}
            {{ dbt_utils.generate_surrogate_key(
                ["'tasty_bytes'", "1", "'franchise_id'", "franchise_id"]
            ) }} as dwh_franchise_id,
            {{ keep_distinct_value('truck_opening_date') }} as truck_opening_date,
            'tasty_bytes' as dwh_source,
            1 as dwh_version,
            'city_name, franchise_id' as dwh_granularity,
            {{ dbt_utils.generate_surrogate_key(
                ['dwh_source', 'dwh_version', 'dwh_granularity', 'primary_city', 'franchise_id']
            ) }} as dwh_truck_id
        from
            truck
        group by
            primary_city,
            franchise_id
    ),

    merged as (
        select * from truck_ids
        union all
        select * from city_franchises
    ),

    recolumned as (
        select
            dwh_truck_id,
            dwh_source,
            dwh_version,
            dwh_granularity,
            * exclude (
                dwh_truck_id,
                dwh_source,
                dwh_version,
                dwh_granularity
            )
        from
            merged
    )

select * from recolumned
