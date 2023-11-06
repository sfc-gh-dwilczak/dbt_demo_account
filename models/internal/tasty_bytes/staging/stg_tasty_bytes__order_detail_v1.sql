with
    source as (
        select *
        from {{ ref('snp_tasty_bytes__order_detail', v=1) }}
        {{ filter_snapshot_current() }}
    ),

    renamed as (
        select
            order_detail_id,
            order_id,
            menu_item_id,
            discount_id,
            line_number,
            quantity,
            unit_price,
            price,
            order_item_discount_amount,
            dbt_scd_id,
            dbt_updated_at,
            dbt_valid_from,
            dbt_valid_to,
            dbt_valid_from as dwh_effective_from
        from
            source
    )

select * from renamed
