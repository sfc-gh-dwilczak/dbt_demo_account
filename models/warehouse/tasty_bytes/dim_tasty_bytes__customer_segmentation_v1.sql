with
    order_header as (select * from {{ ref('stg_tasty_bytes__order_header', v=1) }}),

    metrics as (
        select
            {{ dbt_utils.generate_surrogate_key(
                ["'tasty_bytes'", "1", "'customer_id'", "customer_id"]
            ) }} as dwh_customer_id,
            current_date - max(ordered_at::date) as recency,
            count(*) as frequency,
            sum(order_total) as monetary
        from
            order_header
        group by
            all
    ),

    percentiles as (
        select
            *,
            percent_rank() over (order by recency desc) as recency_percentile,
            percent_rank() over (order by frequency) as frequency_percentile,
            percent_rank() over (order by monetary) as monetary_percentile
        from
            metrics
    ),

    scores as (
        select
            *,
            {%- for metric in ['recency', 'frequency', 'monetary'] %}
            case
                when {{ metric }}_percentile >= 0.8 then 5
                when {{ metric }}_percentile >= 0.6 then 4
                when {{ metric }}_percentile >= 0.4 then 3
                when {{ metric }}_percentile >= 0.2 then 2
                else 1
            end as {{ metric }}_score {%- if not loop.last -%} , {%- endif -%}
            {%- endfor %}
        from
            percentiles
    ),

    segments as (
        select
            *,
            case
                when recency_score <= 2 and frequency_score <= 2 then 'Hibernating'
                when recency_score <= 2 and frequency_score <= 4 then 'At Risk'
                when recency_score <= 2 and frequency_score <= 5 then 'Cannot Lose Them'
                when recency_score <= 3 and frequency_score <= 2 then 'About to Sleep'
                when recency_score <= 3 and frequency_score <= 3 then 'Need Attention'
                when recency_score <= 4 and frequency_score <= 1 then 'Promising'
                when recency_score <= 4 and frequency_score <= 3 then 'Potential Loyalists'
                when recency_score <= 4 and frequency_score <= 5 then 'Loyal Customers'
                when recency_score <= 5 and frequency_score <= 1 then 'New Customers'
                when recency_score <= 5 and frequency_score <= 3 then 'Potential Loyalists'
                else 'Champions'
            end as rfm_segment
        from
            scores
    )

select * from segments
