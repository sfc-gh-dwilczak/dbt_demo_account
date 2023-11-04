with
    warehouse_usage as (select * from {{ ref('stg_snowflake_usage__warehouse', v=1) }}),

    dimensions as (
        select
            {{ dbt_utils.generate_surrogate_key(
                ["'warehouse'", 'dbt_scd_id']
            ) }} as dwh_snowflake_usage_id,
            warehouse_name
        from
            warehouse_usage
    )

select * from dimensions
