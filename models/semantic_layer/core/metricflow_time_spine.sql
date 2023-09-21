with
    customer as (select * from {{ ref('src_tasty_bytes__customer') }}),
    orders as (select * from {{ ref('src_tasty_bytes__order_header') }}),

    start_dates as (
        select
            min(sign_up_date::date) as date_day,
            max(sign_up_date::date) as final_date
        from
            customer
        
        union all

        select
            min(order_ts::date) as date_day,
            max(order_ts::date) as final_date
        from
            orders
    ),

    dates as (
        select
            min(date_day) as date_day,
            max(final_date) as final_date
        from
            start_dates
        
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
