with
    query_usage as (select * from {{ ref('stg_snowflake_usage__query', v=1) }}),
    service_usage as (select * from {{ ref('stg_snowflake_usage__service', v=1) }}),
    storage_usage as (select * from {{ ref('stg_snowflake_usage__storage', v=1)}}),
    warehouse_usage as (select * from {{ ref('stg_snowflake_usage__warehouse', v=1) }}),

    query_metrics as (
        select
            'query' as dwh_source,
            {{ dbt_utils.generate_surrogate_key(
                ['dwh_source', 'dbt_scd_id']
            ) }} as dwh_snowflake_usage_id,
            start_time::date as usage_date,
            null as bytes,
            null as credits_used,
            null as credits_used_compute,
            credits_used_cloud_services,
            null as files,
            total_elapsed_time as elapsed_milliseconds
        from
            query_usage
    ),

    service_metrics as (
        select
            'service' as dwh_source,
            {{ dbt_utils.generate_surrogate_key(
                ['dwh_source', 'dbt_scd_id']
            ) }} as dwh_snowflake_usage_id,
            start_time::date as usage_date,
            bytes,
            credits_used,
            credits_used_compute,
            credits_used_cloud_services,
            files,
            null as elapsed_milliseconds
        from
            service_usage
    ),

    storage_metrics as (
        select
            'storage' as dwh_source,
            {{ dbt_utils.generate_surrogate_key(
                ['dwh_source', 'dbt_scd_id']
            ) }} as dwh_snowflake_usage_id,
            usage_date,
            average_stage_bytes as bytes,
            null as credits_used,
            null as credits_used_compute,
            null as credits_used_cloud_services,
            null as files,
            null as elapsed_milliseconds
        from
            storage_usage
    ),

    warehouse_metrics as (
        select
            'warehouse' as dwh_source,
            {{ dbt_utils.generate_surrogate_key(
                ['dwh_source', 'dbt_scd_id']
            ) }} as dwh_snowflake_usage_id,
            start_time::date as usage_date,
            null as bytes,
            credits_used,
            credits_used_compute,
            credits_used_cloud_services,
            null as files,
            datediff(milliseconds, start_time, end_time) as elapsed_milliseconds
        from
            warehouse_usage
    ),

    merged as (
        select * from query_metrics
        union all
        select * from service_metrics
        union all
        select * from storage_metrics
        union all
        select * from warehouse_metrics
    )

select * from merged
