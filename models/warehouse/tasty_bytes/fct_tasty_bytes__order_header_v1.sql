{{ config(materialized='view') }}

with
    order_header as (select * from {{ ref('stg_tasty_bytes__order_header', v=1) }}),

    ids_mapped as (
        select
            'tasty_bytes' as dwh_source,
            1 as dwh_version,
            'order_id' as dwh_granularity,
            {{ dbt_utils.generate_surrogate_key(
                ['dwh_source', 'dwh_version', 'dwh_granularity', 'order_id']
            ) }} as dwh_order_id,
            {{ dbt_utils.generate_surrogate_key(
                ["dwh_source", "dwh_version", "'truck_id'", "truck_id"]
            ) }} as dwh_truck_id,
            {{ dbt_utils.generate_surrogate_key(
                ["dwh_source", "dwh_version", "'location_id'", "location_id"]
            ) }} as dwh_location_id,
            {{ dbt_utils.generate_surrogate_key(
                ["dwh_source", "dwh_version", "'customer_id'", "customer_id"]
            ) }} as dwh_customer_id,
            discount_id,
            shift_id,
            shift_start_time,
            shift_end_time,
            order_channel,
            ordered_at,
            served_at,
            order_currency,
            order_amount,
            order_tax_amount,
            order_discount_amount,
            order_total
        from
            order_header
    )

select * from ids_mapped
