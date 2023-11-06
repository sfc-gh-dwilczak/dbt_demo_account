with
    source as (select * from {{ ref('snp_tasty_bytes__order_header', v=1) }}),

    renamed as (
        select
            order_id,
            truck_id,
            location_id,
            customer_id,
            discount_id,
            shift_id,
            shift_start_time,
            shift_end_time,
            order_channel,
            order_ts as ordered_at,
            served_ts as served_at,
            order_currency,
            order_amount,
            order_tax_amount,
            order_discount_amount,
            order_total,
            dbt_scd_id,
            dbt_updated_at,
            dbt_valid_from,
            dbt_valid_to,
            dbt_valid_from as dwh_effective_from
        from
            source
    )

select * from renamed
