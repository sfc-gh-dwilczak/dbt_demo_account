with
    query_usage as (select * from {{ ref('stg_snowflake_usage__query', v=1) }}),

    dimensions as (
        select
            {{ dbt_utils.generate_surrogate_key(
                ["'query'", 'dbt_scd_id']
            ) }} as dwh_snowflake_usage_id,
            query_type,
            query_tag,
            query_text,
            database_name,
            "SCHEMA_NAME",
            "USER_NAME",
            role_type,
            role_name,
            warehouse_name,
            warehouse_size,
            warehouse_type,
            warehouse_clusters
        from
            query_usage
    )

select * from dimensions
