with
    tasty_bytes as (select * from {{ ref('stg_tasty_bytes__customer') }}),

    customer_ids as (
        select
            customer_id,
            first_name,
            last_name,
            {{ dbt_utils.generate_surrogate_key(
                ["'tasty_bytes'", "1", "'city_name'", "city_name"]
            ) }} as dwh_location_id,
            city_name,
            country_name,
            postal_code,
            preferred_language,
            gender,
            favourite_brand,
            marital_status,
            children_count,
            sign_up_date,
            birthday_date,
            email,
            phone_number,
            'tasty_bytes' as dwh_source,
            1 as dwh_version,
            'customer_id' as dwh_granularity,
            {{ dbt_utils.generate_surrogate_key(
                ['dwh_source', 'dwh_version', 'dwh_granularity', 'customer_id']
            ) }} as dwh_customer_id
        from
            tasty_bytes
    ),

    customer_names as (
        select
            {{ keep_distinct_value('customer_id') }} as customer_id,
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
                'postal_code',
                'preferred_language',
                'gender',
                'favourite_brand',
                'marital_status',
                'children_count',
                'sign_up_date',
                'birthday_date',
                'email',
                'phone_number'
            ] %}
            {{ keep_distinct_value(column) }} as {{ column }},
            {%- endfor %}
            'tasty_bytes' as dwh_source,
            1 as dwh_version,
            'first_name, last_name' as dwh_granularity,
            {{ dbt_utils.generate_surrogate_key(
                ['dwh_source', 'dwh_version', 'dwh_granularity', 'first_name', 'last_name']
            ) }} as dwh_customer_id
        from
            tasty_bytes
        group by
            first_name, last_name
    ),

    postal_codes as (
        select
            {%- for column in [
                'customer_id',
                'first_name',
                'last_name'
            ] %}
            {{ keep_distinct_value(column) }} as {{ column }},
            {%- endfor %}
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
                'country_name'
            ] %}
            {{ keep_distinct_value(column) }} as {{ column }},
            {%- endfor %}
            postal_code,
            {%- for column in [
                'preferred_language',
                'gender',
                'favourite_brand',
                'marital_status',
                'children_count',
                'sign_up_date',
                'birthday_date',
                'email',
                'phone_number'
            ] %}
            {{ keep_distinct_value(column) }} as {{ column }},
            {%- endfor %}
            'tasty_bytes' as dwh_source,
            1 as dwh_version,
            'postal_code' as dwh_granularity,
            {{ dbt_utils.generate_surrogate_key(
                ['dwh_source', 'dwh_version', 'dwh_granularity', 'postal_code']
            ) }} as dwh_customer_id
        from
            tasty_bytes
        group by
            postal_code
    ),

    emails as (
        select
            {%- for column in [
                'customer_id',
                'first_name',
                'last_name'
            ] %}
            {{ keep_distinct_value(column) }} as {{ column }},
            {%- endfor %}
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
                'postal_code',
                'preferred_language',
                'gender',
                'favourite_brand',
                'marital_status',
                'children_count',
                'sign_up_date',
                'birthday_date'
            ] %}
            {{ keep_distinct_value(column) }} as {{ column }},
            {%- endfor %}
            email,
            {{ keep_distinct_value('phone_number') }} as phone_number,
            'tasty_bytes' as dwh_source,
            1 as dwh_version,
            'email' as dwh_granularity,
            {{ dbt_utils.generate_surrogate_key(
                ['dwh_source', 'dwh_version', 'dwh_granularity', 'email']
            ) }} as dwh_customer_id
        from
            tasty_bytes
        group by
            email
    ),

    phone_numbers as (
        select
            {%- for column in [
                'customer_id',
                'first_name',
                'last_name'
            ] %}
            {{ keep_distinct_value(column) }} as {{ column }},
            {%- endfor %}
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
                'postal_code',
                'preferred_language',
                'gender',
                'favourite_brand',
                'marital_status',
                'children_count',
                'sign_up_date',
                'birthday_date',
                'email'
            ] %}
            {{ keep_distinct_value(column) }} as {{ column }},
            {%- endfor %}
            phone_number,
            'tasty_bytes' as dwh_source,
            1 as dwh_version,
            'phone_number' as dwh_granularity,
            {{ dbt_utils.generate_surrogate_key(
                ['dwh_source', 'dwh_version', 'dwh_granularity', 'phone_number']
            ) }} as dwh_customer_id
        from
            tasty_bytes
        group by
            phone_number
    ),

    merged as (
        select * from customer_ids
        union all
        select * from customer_names
        union all
        select * from postal_codes
        union all
        select * from emails
        union all
        select * from phone_numbers
    ),

    recolumned as (
        select
            dwh_customer_id,
            dwh_source,
            dwh_version,
            dwh_granularity,
            * exclude (
                dwh_customer_id,
                dwh_source,
                dwh_version,
                dwh_granularity
            )
        from
            merged
    )

select * from recolumned
