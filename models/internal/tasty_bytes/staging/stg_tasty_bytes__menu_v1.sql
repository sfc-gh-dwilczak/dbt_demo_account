with
    source as (
        select *
        from {{ ref('snp_tasty_bytes__menu_v1') }}
        {{ filter_snapshot_last_seen('menu_item_id') }}
    ),

    renamed as (
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
            menu_item_health_metrics_obj as health_metrics,
            dbt_scd_id,
            dbt_updated_at,
            dbt_valid_from,
            dbt_valid_to,
            dbt_valid_from as dwh_effective_from
        from
            source
    )

select * from renamed
