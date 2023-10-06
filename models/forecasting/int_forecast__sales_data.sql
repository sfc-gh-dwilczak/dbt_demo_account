-- Predict total sales per date for the next 3 days.
{%- set source_data = "{{ ref('src_forecast__sales_data') }}" -%}
{%- set source_type = 'view' -%}
{%- set predict = 'sales' -%}
{%- set predict_using = 'sold_on' -%}
{%- set predict_days = 3 -%}

{%- set forecast_hook = forecast_model_hooks(
    input_data=source_data,
    input_type=source_type,
    timestamp_colname=predict_using,
    target_colname=predict,
    forecasting_periods=predict_days
) -%}



select
    null::timestamp_ntz as ts,
    null::float         as forecast,
    null::float         as lower_bound,
    null::float         as upper_bound
where
    false
