with
    source as (select * from {{ ref('scd_snowflake__query_history_v1') }}),

    filtered as (
        select
            *
        from
            source
        where
            dbt_valid_to is null
    ),

    renamed as (
        select
            query_id,
            query_text,
            database_id,
            database_name,
            "SCHEMA_ID",
            "SCHEMA_NAME",
            query_type,
            session_id,
            "USER_NAME",
            role_name,
            warehouse_id,
            warehouse_name,
            warehouse_size,
            warehouse_type,
            cluster_number,
            query_tag,
            execution_status,
            error_code,
            "ERROR_MESSAGE",
            start_time,
            end_time,
            total_elapsed_time,
            bytes_scanned,
            percentage_scanned_from_cache,
            bytes_written,
            bytes_written_to_result,
            bytes_read_from_result,
            rows_produced,
            rows_inserted,
            rows_updated,
            rows_deleted,
            rows_unloaded,
            bytes_deleted,
            partitions_scanned,
            partitions_total,
            bytes_spilled_to_local_storage,
            bytes_spilled_to_remote_storage,
            bytes_sent_over_the_network,
            compilation_time,
            execution_time,
            queued_provisioning_time,
            queued_repair_time,
            queued_overload_time,
            transaction_blocked_time,
            outbound_data_transfer_cloud,
            outbound_data_transfer_region,
            outbound_data_transfer_bytes,
            inbound_data_transfer_cloud,
            inbound_data_transfer_region,
            inbound_data_transfer_bytes,
            list_external_files_time,
            credits_used_cloud_services,
            release_version,
            external_function_total_invocations,
            external_function_total_sent_rows,
            external_function_total_received_rows,
            external_function_total_sent_bytes,
            external_function_total_received_bytes,
            query_load_percent,
            is_client_generated_statement,
            query_acceleration_bytes_scanned,
            query_acceleration_partitions_scanned,
            query_acceleration_upper_limit_scale_factor,
            transaction_id,
            child_queries_wait_time,
            role_type,
            query_hash,
            query_hash_version,
            query_parameterized_hash,
            query_parameterized_hash_version,
            dbt_scd_id,
            dbt_updated_at,
            dbt_valid_from,
            dbt_valid_to,
            start_time as dwh_effective_from
        from
            filtered
    )

select * from renamed
