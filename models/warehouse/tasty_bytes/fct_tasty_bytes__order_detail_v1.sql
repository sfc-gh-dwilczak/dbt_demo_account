with
    order_detail as (select * from {{ ref('stg_tasty_bytes__order_detail', v=1) }}),

    ids_mapped as (
        select
            'tasty_bytes' as dwh_source,
            1 as dwh_version,
            'order_detail_id' as dwh_granularity,
            {{ dbt_utils.generate_surrogate_key(
                ['dwh_source', 'dwh_version', 'dwh_granularity', 'order_detail_id']
            ) }} as dwh_order_detail_id,
            {{ dbt_utils.generate_surrogate_key(
                ["dwh_source", "dwh_version", "'order_id'", "order_id"]
            ) }} as dwh_order_id,
            {{ dbt_utils.generate_surrogate_key(
                ["dwh_source", "dwh_version", "'menu_item_id'", "menu_item_id"]
            ) }} as dwh_item_id,
            discount_id,
            line_number,
            quantity,
            unit_price,
            price,
            order_item_discount_amount
        from
            order_detail
    )

select * from ids_mapped
