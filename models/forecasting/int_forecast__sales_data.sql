-- Predict total sales per date for the next 3 days.
{%- set forecasting_periods = 3 -%}

{%- set train_on -%}
select
    date,
    sum(sales) as sales
from
    {{ ref('src_forecast__sales_data') }}
group by
    all
{%- endset -%}

{%- set forecast = forecast_model_hooks(
    input_data=train_on,
    input_type='query',
    timestamp_colname='date',
    target_colname='sales',
    forecasting_periods=forecasting_periods
) -%}

{{ config(post_hook=forecast) }}

select
    null::timestamp_ntz as ts,
    null::float         as forecast,
    null::float         as lower_bound,
    null::float         as upper_bound
where
    false
