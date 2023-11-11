with
    franchise as (select * from {{ ref('stg_tasty_bytes__franchise', v=1) }}),
    location as (select * from {{ ref('stg_tasty_bytes__location', v=1) }}),
    menu as (select * from {{ ref('stg_tasty_bytes__menu', v=1) }}),
    order_detail as (select * from {{ ref('stg_tasty_bytes__order_detail', v=1) }}),
    order_header as (select * from {{ ref('stg_tasty_bytes__order_header', v=1) }}),
    truck as (select * from {{ ref('stg_tasty_bytes__truck', v=1) }}),

    aggregated as (
        select distinct
            truck_id,
            truck_brand_name,
            location_id
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

    city_brand_names as (
        select distinct
            truck.franchise_id,
            aggregated.truck_brand_name,
            location.city_name
        from
            aggregated
        inner join
            location
                using(location_id)
        inner join
            truck
                using(truck_id)
    ),

    franchise_brand_names as (
        select distinct
            franchise_id,
            truck_brand_name
        from
            city_brand_names
    ),

    city_franchises as (
        select
            franchise_id,
            first_name,
            last_name,
            {{ dbt_utils.generate_surrogate_key(
                ["'tasty_bytes'", "1", "'city_name'", "city_name"]
            ) }} as dwh_location_id,
            city_name,
            country_name,
            email,
            phone_number,
            truck_brand_name,
            'tasty_bytes' as dwh_source,
            1 as dwh_version,
            'city_name, franchise_id' as dwh_granularity,
            {{ dbt_utils.generate_surrogate_key(
                ['dwh_source', 'dwh_version', 'dwh_granularity', 'city_name', 'franchise_id']
            ) }} as dwh_franchise_id
        from
            franchise
        full outer join
            (
                select
                    city_name,
                    franchise_id,
                    case count(*)
                        when 1
                        then any_value(truck_brand_name)
                    end as truck_brand_name
                from
                    city_brand_names
                group by
                    all
            )
                using(city_name, franchise_id)
    ),

    franchise_ids as (
        select
            franchise_id,
            {{ keep_distinct_value('first_name') }} as first_name,
            {{ keep_distinct_value('last_name') }} as last_name,
            case
                when min(city_name) = max(city_name)
                then {{ dbt_utils.generate_surrogate_key(
                        ["'tasty_bytes'", "1", "'city_name'", "min(city_name)"]
                    ) }}
                when min(country_name) = max(country_name)
                then {{ dbt_utils.generate_surrogate_key([
                        "'tasty_bytes'",
                        "1",
                        "'country_name'",
                        "case min(country_name) "
                            ~ "when 'United Kingdom' "
                            ~ "then 'England' "
                            ~ "else min(country_name) "
                        ~ "end"
                    ]) }}
            end as dwh_location_id,
            {%- for column in [
                'city_name',
                'country_name',
                'email',
                'phone_number',
                'truck_brand_name'
            ] %}
            {{ keep_distinct_value(column) }} as {{ column }},
            {%- endfor %}
            'tasty_bytes' as dwh_source,
            1 as dwh_version,
            'franchise_id' as dwh_granularity,
            {{ dbt_utils.generate_surrogate_key(
                ['dwh_source', 'dwh_version', 'dwh_granularity', 'franchise_id']
            ) }} as dwh_franchise_id
        from
            franchise
        natural full outer join
            franchise_brand_names
        group by
            franchise_id
    ),

    owner_names as (
        select
            {{ keep_distinct_value('franchise_id') }} as franchise_id,
            first_name,
            last_name,
            case
                when min(city_name) = max(city_name)
                then {{ dbt_utils.generate_surrogate_key(
                        ["'tasty_bytes'", "1", "'city_name'", "min(city_name)"]
                    ) }}
                when min(country_name) = max(country_name)
                then {{ dbt_utils.generate_surrogate_key([
                        "'tasty_bytes'",
                        "1",
                        "'country_name'",
                        "case min(country_name) "
                            ~ "when 'United Kingdom' "
                            ~ "then 'England' "
                            ~ "else min(country_name) "
                        ~ "end"
                    ]) }}
            end as dwh_location_id,
            {%- for column in [
                'city_name',
                'country_name',
                'email',
                'phone_number',
                'truck_brand_name'
            ] %}
            {{ keep_distinct_value(column) }} as {{ column }},
            {%- endfor %}
            'tasty_bytes' as dwh_source,
            1 as dwh_version,
            'first_name, last_name' as dwh_granularity,
            {{ dbt_utils.generate_surrogate_key([
                'dwh_source',
                'dwh_version',
                'dwh_granularity',
                'first_name',
                'last_name'
            ]) }} as dwh_franchise_id
        from
            franchise
        natural left join
            franchise_brand_names
        group by
            first_name,
            last_name
    ),

    emails as (
        select
            {{ keep_distinct_value('franchise_id') }} as franchise_id,
            {{ keep_distinct_value('first_name') }} as first_name,
            {{ keep_distinct_value('last_name') }} as last_name,
            case
                when min(city_name) = max(city_name)
                then {{ dbt_utils.generate_surrogate_key(
                        ["'tasty_bytes'", "1", "'city_name'", "min(city_name)"]
                    ) }}
                when min(country_name) = max(country_name)
                then {{ dbt_utils.generate_surrogate_key([
                        "'tasty_bytes'",
                        "1",
                        "'country_name'",
                        "case min(country_name) "
                            ~ "when 'United Kingdom' "
                            ~ "then 'England' "
                            ~ "else min(country_name) "
                        ~ "end"
                    ]) }}
            end as dwh_location_id,
            {{ keep_distinct_value('city_name') }} as city_name,
            {{ keep_distinct_value('country_name') }} as country_name,
            email,
            {{ keep_distinct_value('phone_number') }} as phone_number,
            {{ keep_distinct_value('truck_brand_name') }} as truck_brand_name,
            'tasty_bytes' as dwh_source,
            1 as dwh_version,
            'email' as dwh_granularity,
            {{ dbt_utils.generate_surrogate_key(
                ['dwh_source', 'dwh_version', 'dwh_granularity', 'email']
            ) }} as dwh_franchise_id
        from
            franchise
        natural left join
            franchise_brand_names
        group by
            email
    ),

    phone_numbers as (
        select
            {{ keep_distinct_value('franchise_id') }} as franchise_id,
            {{ keep_distinct_value('first_name') }} as first_name,
            {{ keep_distinct_value('last_name') }} as last_name,
            case
                when min(city_name) = max(city_name)
                then {{ dbt_utils.generate_surrogate_key(
                        ["'tasty_bytes'", "1", "'city_name'", "min(city_name)"]
                    ) }}
                when min(country_name) = max(country_name)
                then {{ dbt_utils.generate_surrogate_key([
                        "'tasty_bytes'",
                        "1",
                        "'country_name'",
                        "case min(country_name) "
                            ~ "when 'United Kingdom' "
                            ~ "then 'England' "
                            ~ "else min(country_name) "
                        ~ "end"
                    ]) }}
            end as dwh_location_id,
            {{ keep_distinct_value('city_name') }} as city_name,
            {{ keep_distinct_value('country_name') }} as country_name,
            {{ keep_distinct_value('email') }} as email,
            phone_number,
            {{ keep_distinct_value('truck_brand_name') }} as truck_brand_name,
            'tasty_bytes' as dwh_source,
            1 as dwh_version,
            'phone_number' as dwh_granularity,
            {{ dbt_utils.generate_surrogate_key(
                ['dwh_source', 'dwh_version', 'dwh_granularity', 'phone_number']
            ) }} as dwh_franchise_id
        from
            franchise
        natural left join
            franchise_brand_names
        group by
            phone_number
    ),

    truck_brand_names as (
        select
            {{ keep_distinct_value('franchise_id') }} as franchise_id,
            {{ keep_distinct_value('first_name') }} as first_name,
            {{ keep_distinct_value('last_name') }} as last_name,
            case
                when min(city_name) = max(city_name)
                then {{ dbt_utils.generate_surrogate_key(
                        ["'tasty_bytes'", "1", "'city_name'", "min(city_name)"]
                    ) }}
                when min(country_name) = max(country_name)
                then {{ dbt_utils.generate_surrogate_key([
                        "'tasty_bytes'",
                        "1",
                        "'country_name'",
                        "case min(country_name) "
                            ~ "when 'United Kingdom' "
                            ~ "then 'England' "
                            ~ "else min(country_name) "
                        ~ "end"
                    ]) }}
            end as dwh_location_id,
            {%- for column in [
                'city_name',
                'country_name',
                'email',
                'phone_number'
            ] %}
            {{ keep_distinct_value(column) }} as {{ column }},
            {%- endfor %}
            truck_brand_name,
            'tasty_bytes' as dwh_source,
            1 as dwh_version,
            'truck_brand_name' as dwh_granularity,
            {{ dbt_utils.generate_surrogate_key(
                ['dwh_source', 'dwh_version', 'dwh_granularity', 'truck_brand_name']
            ) }} as dwh_franchise_id
        from
            franchise
        natural right join
            franchise_brand_names
        group by
            truck_brand_name
    ),

    merged as (
        select * from city_franchises
        union all
        select * from franchise_ids
        union all
        select * from owner_names
        union all
        select * from emails
        union all
        select * from phone_numbers
        union all
        select * from truck_brand_names
    ),

    recolumned as (
        select
            dwh_franchise_id,
            dwh_source,
            dwh_version,
            dwh_granularity,
            * exclude (
                dwh_franchise_id,
                dwh_source,
                dwh_version,
                dwh_granularity
            )
        from
            merged
    )

select * from recolumned
