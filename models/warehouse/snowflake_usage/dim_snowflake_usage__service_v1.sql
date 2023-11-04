with
    service_usage as (select * from {{ ref('stg_snowflake_usage__service', v=1) }}),

    dimensions as (
        select
            {{ dbt_utils.generate_surrogate_key(
                ["'service'", 'dbt_scd_id']
            ) }} as dwh_snowflake_usage_id,
            entity_id,
            service_type,
            service_name
        from
            service_usage
    )

select * from dimensions
