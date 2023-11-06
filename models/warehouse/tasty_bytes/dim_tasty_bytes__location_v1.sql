with
    tasty_bytes_city as (select * from {{ ref('stg_tasty_bytes__city', v=1) }}),
    tasty_bytes_location as (select * from {{ ref('stg_tasty_bytes__location', v=1) }}),
    
    tasty_bytes_truck as (
        select distinct
            primary_city as city_name,
            country_region_name,
            country_region_iso_code
        from
            {{ ref('stg_tasty_bytes__truck', v=1) }}
    ),

    city_ids as (
        select
            null as location_id,
            null as place_key,
            null as location_name,
            null as local_region_name,
            city_id,
            city_name,
            city_population,
            country_region_name,
            country_region_iso_code,
            country_id,
            country_name,
            country_iso_code,
            currency_iso_code,
            'tasty_bytes' as dwh_source,
            1 as dwh_version,
            'city_id' as dwh_granularity,
            {{ dbt_utils.generate_surrogate_key(
                ['dwh_source', 'dwh_version', 'dwh_granularity', 'city_id']
            ) }} as dwh_location_id
        from
            tasty_bytes_city
        left join
            tasty_bytes_truck
                using(city_name)
    ),

    city_names as (
        select
            location_id,
            place_key,
            location_name,
            local_region_name,
            city_id,
            city_name,
            city_population,
            country_region_name,
            country_region_iso_code,
            country_id,
            country_name,
            country_iso_code,
            currency_iso_code,
            'tasty_bytes' as dwh_source,
            1 as dwh_version,
            'city_name' as dwh_granularity,
            {{ dbt_utils.generate_surrogate_key(
                ["'tasty_bytes'", "1", "'city_name'", "city_name"]
            ) }} as dwh_location_id
        from
            city_ids
    ),

    location_ids as (
        select
            tasty_bytes_location.location_id,
            tasty_bytes_location.place_key,
            tasty_bytes_location.location_name,
            tasty_bytes_location.local_region_name,
            city_ids.city_id,
            city_name,
            city_ids.city_population,
            city_ids.country_region_name,
            city_ids.country_region_iso_code,
            city_ids.country_id,
            tasty_bytes_location.country_name,
            tasty_bytes_location.country_iso_code,
            city_ids.currency_iso_code,
            'tasty_bytes' as dwh_source,
            1 as dwh_version,
            'location_id' as dwh_granularity,
            {{ dbt_utils.generate_surrogate_key(
                ["'tasty_bytes'", "1", "'location_id'", "tasty_bytes_location.location_id"]
            ) }} as dwh_location_id
        from
            tasty_bytes_location
        left join
            city_ids
                using(city_name)
    ),

    place_keys as (
        select
            location_id,
            place_key,
            location_name,
            local_region_name,
            city_id,
            city_name,
            city_population,
            country_region_name,
            country_region_iso_code,
            country_id,
            country_name,
            country_iso_code,
            currency_iso_code,
            'tasty_bytes' as dwh_source,
            1 as dwh_version,
            'place_key' as dwh_granularity,
            {{ dbt_utils.generate_surrogate_key(
                ["'tasty_bytes'", "1", "'place_key'", "place_key"]
            ) }} as dwh_location_id
        from
            location_ids
    ),

    location_names as (
        select
            {{ keep_distinct_value('location_id') }} as location_id,
            {{ keep_distinct_value('place_key') }} as place_key,
            location_name,
            {%- for column in [
                'local_region_name',
                'city_id',
                'city_name',
                'city_population',
                'country_region_name',
                'country_region_iso_code',
                'country_id',
                'country_name',
                'country_iso_code',
                'currency_iso_code'
            ] %}
            {{ keep_distinct_value(column) }} as {{ column }},
            {%- endfor %}
            'tasty_bytes' as dwh_source,
            1 as dwh_version,
            'location_name' as dwh_granularity,
            {{ dbt_utils.generate_surrogate_key(
                ["'tasty_bytes'", "1", "'location_name'", "location_name"]
            ) }} as dwh_location_id
        from
            location_ids
        group by
            location_name
    ),

    local_region_names as (
        select
            {{ keep_distinct_value('location_id') }} as location_id,
            {{ keep_distinct_value('place_key') }} as place_key,
            {{ keep_distinct_value('location_name') }} as location_name,
            local_region_name,
            {%- for column in [
                'city_id',
                'city_name',
                'city_population',
                'country_region_name',
                'country_region_iso_code',
                'country_id',
                'country_name',
                'country_iso_code',
                'currency_iso_code'
            ] %}
            {{ keep_distinct_value(column) }} as {{ column }},
            {%- endfor %}
            'tasty_bytes' as dwh_source,
            1 as dwh_version,
            'local_region_name' as dwh_granularity,
            {{ dbt_utils.generate_surrogate_key(
                ["'tasty_bytes'", "1", "'local_region_name'", "local_region_name"]
            ) }} as dwh_location_id
        from
            location_ids
        group by
            local_region_name
    ),

    country_region_names as (
        select
            null as location_id,
            null as place_key,
            null as location_name,
            null as local_region_name,
            null as city_id,
            null as city_name,
            null as city_population,
            country_region_name,
            {%- for column in [
                'country_region_iso_code',
                'country_id',
                'country_name',
                'country_iso_code',
                'currency_iso_code'
            ] %}
            {{ keep_distinct_value(column) }} as {{ column }},
            {%- endfor %}
            'tasty_bytes' as dwh_source,
            1 as dwh_version,
            'country_region_name' as dwh_granularity,
            {{ dbt_utils.generate_surrogate_key(
                ["'tasty_bytes'", "1", "'country_region_name'", "country_region_name"]
            ) }} as dwh_location_id
        from
            city_ids
        group by
            country_region_name
    ),

    country_region_iso_codes as (
        select
            null as location_id,
            null as place_key,
            null as location_name,
            null as local_region_name,
            null as city_id,
            null as city_name,
            null as city_population,
            {{ keep_distinct_value('country_region_name') }} as country_region_name,
            country_region_iso_code,
            {%- for column in [
                'country_id',
                'country_name',
                'country_iso_code',
                'currency_iso_code'
            ] %}
            {{ keep_distinct_value(column) }} as {{ column }},
            {%- endfor %}
            'tasty_bytes' as dwh_source,
            1 as dwh_version,
            'country_region_iso_code' as dwh_granularity,
            {{ dbt_utils.generate_surrogate_key(
                ["'tasty_bytes'", "1", "'country_region_iso_code'", "country_region_iso_code"]
            ) }} as dwh_location_id
        from
            city_ids
        group by
            country_region_iso_code
    ),

    country_ids as (
        select
            null as location_id,
            null as place_key,
            null as location_name,
            null as local_region_name,
            null as city_id,
            null as city_name,
            null as city_population,
            null as country_region_name,
            null as country_region_iso_code,
            country_id,
            any_value(country_name) as country_name,
            any_value(country_iso_code) as country_iso_code,
            any_value(currency_iso_code) as currency_iso_code,
            'tasty_bytes' as dwh_source,
            1 as dwh_version,
            'country_id' as dwh_granularity,
            {{ dbt_utils.generate_surrogate_key(
                ["'tasty_bytes'", "1", "'country_id'", "country_id"]
            ) }} as dwh_location_id
        from
            tasty_bytes_city
        group by
            country_id
    ),

    country_names as (
        select
            location_id,
            place_key,
            location_name,
            local_region_name,
            city_id,
            city_name,
            city_population,
            country_region_name,
            country_region_iso_code,
            country_id,
            country_name,
            country_iso_code,
            currency_iso_code,
            'tasty_bytes' as dwh_source,
            1 as dwh_version,
            'country_name' as dwh_granularity,
            {{ dbt_utils.generate_surrogate_key(
                ["'tasty_bytes'", "1", "'country_name'", "country_name"]
            ) }} as dwh_location_id
        from
            country_ids
    ),

    country_iso_codes as (
        select
            location_id,
            place_key,
            location_name,
            local_region_name,
            city_id,
            city_name,
            city_population,
            country_region_name,
            country_region_iso_code,
            country_id,
            country_name,
            country_iso_code,
            currency_iso_code,
            'tasty_bytes' as dwh_source,
            1 as dwh_version,
            'country_iso_code' as dwh_granularity,
            {{ dbt_utils.generate_surrogate_key(
                ["'tasty_bytes'", "1", "'country_iso_code'", "country_iso_code"]
            ) }} as dwh_location_id
        from
            country_ids
    ),

    merged as (
        select * from city_ids
        union all
        select * from city_names
        union all
        select * from location_ids
        union all
        select * from place_keys
        union all
        select * from location_names
        union all
        select * from local_region_names
        union all
        select * from country_region_names
        union all
        select * from country_region_iso_codes
        union all
        select * from country_ids
        union all
        select * from country_names
        union all
        select * from country_iso_codes
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
