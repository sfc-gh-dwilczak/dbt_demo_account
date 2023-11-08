with
    menu as (select * from {{ ref('stg_tasty_bytes__menu', v=1) }}),
    order_detail as (select * from {{ ref('stg_tasty_bytes__order_detail', v=1) }}),
    order_header as (select * from {{ ref('stg_tasty_bytes__order_header', v=1) }}),
    truck as (select * from {{ ref('stg_tasty_bytes__truck', v=1) }}),

    truck_brand_names as (
        select distinct
            truck_id,
            truck_brand_name
        from
            order_detail
        left join
            order_header
                using(order_id)
        left join
            menu
                using(menu_item_id)
        where
            truck_id is not null
    ),

    truck_ids as (
        select
            truck_id,
            any_value(menu_type_id) as menu_type_id,
            {{ dbt_utils.generate_surrogate_key(
                ["'tasty_bytes'", "1", "'city_name'", "any_value(primary_city)"]
            ) }} as dwh_location_id,
            {%- for column in [
                'primary_city',
                'country_region_name',
                'country_region_iso_code',
                'country_name',
                'country_iso_code',
                'franchise_flag',
                'truck_year',
                'truck_make',
                'truck_model',
                'ev_flag'
            ] %}
            any_value({{ column }}) as {{ column }},
            {%- endfor %}
            {{ dbt_utils.generate_surrogate_key(
                ["'tasty_bytes'", "1", "'franchise_id'", "any_value(franchise_id)"]
            ) }} as dwh_franchise_id,
            any_value(truck_opening_date) as truck_opening_date,
            {{ keep_distinct_value('truck_brand_name') }} as truck_brand_name,
            'tasty_bytes' as dwh_source,
            1 as dwh_version,
            'truck_id' as dwh_granularity,
            {{ dbt_utils.generate_surrogate_key(
                ['dwh_source', 'dwh_version', 'dwh_granularity', 'truck_id']
            ) }} as dwh_truck_id
        from
            truck
        natural full outer join
            truck_brand_names
        group by
            truck_id
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
            {{ keep_distinct_value('truck_brand_name') }} as truck_brand_name,
            'tasty_bytes' as dwh_source,
            1 as dwh_version,
            'city_name, franchise_id' as dwh_granularity,
            {{ dbt_utils.generate_surrogate_key(
                ['dwh_source', 'dwh_version', 'dwh_granularity', 'primary_city', 'franchise_id']
            ) }} as dwh_truck_id
        from
            truck
        natural full outer join
            truck_brand_names
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
