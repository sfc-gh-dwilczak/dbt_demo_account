{% if target.name == 'production' %}
    {{ config(database='data_warehouse', schema='semantic_layer', alias='metricflow_time_spine') }}
{% endif -%}

with
    snowflake_usage as (select * from {{ ref('fct_snowflake_usage__metrics') }}),
    customer as (select * from {{ ref('stg_tasty_bytes__customer', v=1) }}),
    orders as (select * from {{ ref('stg_tasty_bytes__order_header', v=1) }}),

    date_ranges as (
        select
            min(usage_date::date) as start_on,
            max(usage_date::date) as stop_on
        from
            snowflake_usage
        
        union all

        select
            min(sign_up_date::date) as start_on,
            max(sign_up_date::date) as stop_on
        from
            customer
        
        union all

        select
            min(ordered_at::date) as start_on,
            max(ordered_at::date) as stop_on
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
