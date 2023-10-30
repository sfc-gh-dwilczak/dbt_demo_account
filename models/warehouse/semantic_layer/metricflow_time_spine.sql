{% if target.name == 'production' %}
    {{ config(database='data_warehouse', schema='semantic_layer', alias='metricflow_time_spine') }}
{% endif -%}

with
    --future_features as (select * from {# ref('stg_forecast__future_features') #}),
    --sales_data as (select * from {# ref('stg_forecast__sales_data') #}),
    metering_history as (select * from {{ ref('stg_snowflake__metering_history') }}),
    query_history as (select * from {{ ref('stg_snowflake__query_history') }}),
    stage_storage_usage_history as (select * from {{ ref('stg_snowflake__stage_storage_usage_history') }}),
    warehouse_metering_history as (select * from {{ ref('stg_snowflake__warehouse_metering_history') }}),
    customer as (select * from {{ ref('stg_tasty_bytes__customer') }}),
    orders as (select * from {{ ref('stg_tasty_bytes__order_header') }}),

    date_ranges as (
        /*
        select
            min("DATE"::date) as start_on,
            max("DATE"::date) as stop_on
        from
            future_features
        
        union all

        select
            min(sold_on::date) as start_on,
            max(sold_on::date) as stop_on
        from
            sales_data
        
        union all
        */
        select
            min(start_time::date) as start_on,
            max(end_time::date) as stop_on
        from
            metering_history
        
        union all

        select
            min(start_time::date) as start_on,
            max(end_time::date) as stop_on
        from
            query_history
        
        union all

        select
            min(usage_date::date) as start_on,
            max(usage_date::date) as stop_on
        from
            stage_storage_usage_history
        
        union all

        select
            min(start_time::date) as start_on,
            max(end_time::date) as stop_on
        from
            warehouse_metering_history
        
        union all

        select
            min(sign_up_date::date) as start_on,
            max(sign_up_date::date) as stop_on
        from
            customer
        
        union all

        select
            min(order_ts::date) as start_on,
            max(order_ts::date) as stop_on
        from
            orders
    ),

    total_date_range as (
        select
            min(start_on) as start_on,
            max(stop_on) as stop_on
        from
            date_ranges
    ),

    dates as (
        select
            min(start_on) as date_day,
            max(stop_on) as final_date
        from
            date_ranges
        
        union all

        select
            dates.date_day + interval '1 day' as date_day,
            dates.final_date
        from
            dates
        where
            dates.date_day < dates.final_date
    )

select date_day from dates
