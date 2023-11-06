with
    franchise as (select * from {{ ref('stg_tasty_bytes__franchise', v=1) }}),

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
                'phone_number'
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
                'phone_number'
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
            'tasty_bytes' as dwh_source,
            1 as dwh_version,
            'email' as dwh_granularity,
            {{ dbt_utils.generate_surrogate_key(
                ['dwh_source', 'dwh_version', 'dwh_granularity', 'email']
            ) }} as dwh_franchise_id
        from
            franchise
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
            'tasty_bytes' as dwh_source,
            1 as dwh_version,
            'phone_number' as dwh_granularity,
            {{ dbt_utils.generate_surrogate_key(
                ['dwh_source', 'dwh_version', 'dwh_granularity', 'phone_number']
            ) }} as dwh_franchise_id
        from
            franchise
        group by
            phone_number
    ),

    merged as (
        select * from franchise_ids
        union all
        select * from owner_names
        union all
        select * from emails
        union all
        select * from phone_numbers
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
