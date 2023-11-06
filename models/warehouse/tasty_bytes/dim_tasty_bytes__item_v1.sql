with
    menu as (select * from {{ ref('stg_tasty_bytes__menu', v=1) }}),

    menu_ids as (
        select
            menu_id,
            menu_type_id,
            menu_type,
            truck_brand_name,
            menu_item_id,
            menu_item_name,
            item_category,
            item_subcategory,
            cost_of_goods_usd,
            sale_price_usd,
            health_metrics,
            'tasty_bytes' as dwh_source,
            1 as dwh_version,
            'menu_id' as dwh_granularity,
            {{ dbt_utils.generate_surrogate_key(
                ['dwh_source', 'dwh_version', 'dwh_granularity', 'menu_id']
            ) }} as dwh_item_id
        from
            menu
    ),

    menu_item_ids as (
        select
            menu_id,
            menu_type_id,
            menu_type,
            truck_brand_name,
            menu_item_id,
            menu_item_name,
            item_category,
            item_subcategory,
            cost_of_goods_usd,
            sale_price_usd,
            health_metrics,
            'tasty_bytes' as dwh_source,
            1 as dwh_version,
            'menu_item_id' as dwh_granularity,
            {{ dbt_utils.generate_surrogate_key(
                ['dwh_source', 'dwh_version', 'dwh_granularity', 'menu_item_id']
            ) }} as dwh_item_id
        from
            menu
    ),

    menu_item_names as (
        select
            {%- for column in [
                'menu_id',
                'menu_type_id',
                'menu_type',
                'truck_brand_name',
                'menu_item_id'
            ] %}
            {{ keep_distinct_value(column) }} as {{ column }},
            {%- endfor %}
            menu_item_name,
            {%- for column in [
                'item_category',
                'item_subcategory',
                'cost_of_goods_usd',
                'sale_price_usd',
                'health_metrics'
            ] %}
            {{ keep_distinct_value(column) }} as {{ column }},
            {%- endfor %}
            'tasty_bytes' as dwh_source,
            1 as dwh_version,
            'menu_item_name' as dwh_granularity,
            {{ dbt_utils.generate_surrogate_key(
                ['dwh_source', 'dwh_version', 'dwh_granularity', 'menu_item_name']
            ) }} as dwh_item_id
        from
            menu
        group by
            menu_item_name
    ),

    merged as (
        select * from menu_ids
        union all
        select * from menu_item_ids
        union all
        select * from menu_item_names
    ),

    recolumned as (
        select
            dwh_item_id,
            dwh_source,
            dwh_version,
            dwh_granularity,
            * exclude (
                dwh_item_id,
                dwh_source,
                dwh_version,
                dwh_granularity
            )
        from
            merged
    )

select * from recolumned
