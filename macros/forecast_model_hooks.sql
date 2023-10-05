{#
Generates hooks which can be used to create Snowflake
forecast models as DBT models.

Forecasting on a Single Series:

-- Predict total sales per date for the next 3 days.
{%- set train_on -%}
select
    date,
    sum(sales) as sales
from
    {{ ref('sales') }}
group by
    all
{%- endset -%}

{%- set forecast = forecast_model_hooks(
    input_data=train_on,
    input_type='query',
    timestamp_colname='date',
    target_colname='sales',
    forecasting_periods=3
) -%}

{{ config(post_hook=forecast) }}

select
    null::timestamp_ntz as ts,
    null::float         as forecast,
    null::float         as lower_bound,
    null::float         as upper_bound
where
    false


Forecasting on a Single Series with Exogenous Variables:

-- Predict total sales per date using temperature
-- for tomorrow assuming a temperature of 52 degrees.
{%- set train_on -%}
select
    date,
    temperature,
    sum(sales) as sales
from
    {{ ref('sales') }}
group by
    all
{%- endset -%}

{%- set forecast_using -%}
select
    current_date + interval '1 day' as date,
    52 as temperature
{%- endset -%}

{%- set forecast = forecast_model_hooks(
    input_data=train_on,
    input_type='query',
    timestamp_colname='date',
    target_colname='sales',
    forecast_input_data=forecast_using,
    forecast_input_type='query',
    forecast_timestamp_colname='date'
) -%}

{{ config(post_hook=forecast) }}

select
    null::timestamp_ntz as ts,
    null::float         as forecast,
    null::float         as lower_bound,
    null::float         as upper_bound
where
    false


Forecasting on Multiple Series:

-- Predict total sales per store per date for the next 3 days.
{%- set train_on -%}
select
    store_id,
    date,
    sum(sales) as sales
from
    {{ ref('sales') }}
group by
    all
{%- endset -%}

{%- set forecast = forecast_model_hooks(
    input_data=train_on,
    input_type='query',
    timestamp_colname='date',
    target_colname='sales',
    series_colname='store_id',
    forecasting_periods=3
) -%}

{{ config(post_hook=forecast) }}

select
    -- Assuming store_id is an integer.
    null::integer       as series,
    null::timestamp_ntz as ts,
    null::float         as forecast,
    null::float         as lower_bound,
    null::float         as upper_bound
where
    false


Forecasting on Multiple Series with Exogenous Variables:

-- Predict total sales per store per date using temperature
-- for tomorrow assuming a temperature of 51 degrees for store 1
-- and 54 degrees for store 2.
{%- set train_on -%}
select
    store_id,
    date,
    temperature,
    sum(sales) as sales
from
    {{ ref('sales') }}
group by
    all
{%- endset -%}

{%- set forecast_using -%}
select
    1 as store_id,
    current_date + interval '1 day' as date,
    51 as temperature

union all

select
    2 as store_id,
    current_date + interval '1 day' as date,
    54 as temperature
{%- endset -%}

{%- set forecast = forecast_model_hooks(
    input_data=train_on,
    input_type='query',
    timestamp_colname='date',
    target_colname='sales',
    series_colname='store_id',
    forecast_input_data=forecast_using,
    forecast_input_type='query',
    forecast_timestamp_colname='date',
    forecast_series_colname='store_id'
) -%}

{{ config(post_hook=forecast) }}

select
    -- Assuming store_id is an integer.
    null::integer       as series,
    null::timestamp_ntz as ts,
    null::float         as forecast,
    null::float         as lower_bound,
    null::float         as upper_bound
where
    false
#}

{% macro forecast_model_hooks(
    input_data,
    input_type,
    timestamp_colname,
    target_colname,
    series_colname=none,
    forecasting_periods=none,
    forecast_input_data=none,
    forecast_input_type=none,
    forecast_timestamp_colname=none,
    forecast_series_colname=none
) %}

{% set input_type = input_type.upper() %}

{% if input_type == "QUERY" %}
    {% set input_data = "SYSTEM$QUERY_REFERENCE('" ~ input_data ~ "')" %}
{% elif input_type in ["TABLE", "VIEW"] %}
    {% set input_data = "SYSTEM$REFERENCE('" ~ input_type ~ "', '" ~ input_data ~ "')" %}
{% endif %}

{% if series_colname is none %}
    {% set series_colname = "" %}
{% else %}
    {% set series_colname = ",\n    SERIES_COLNAME => '" ~ series_colname ~ "'" %}
{% endif %}

{% set ml_model %}
{{ (this | string).replace(".", "__") }}__ML_MODEL
{% endset %}

{% set train %}
CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {{ ml_model }}(
    INPUT_DATA => {{ input_data }},
    TIMESTAMP_COLNAME => '{{ timestamp_colname }}',
    TARGET_COLNAME => '{{ target_colname }}'
    {{- series_colname }}
);
{% endset %}

{% if forecasting_periods is not none %}
    {% set forecast = "FORECASTING_PERIODS => " ~ forecasting_periods %}
{% else %}
    {% set forecast_input_type = forecast_input_type.upper() %}

    {% if forecast_input_type == "QUERY" %}
        {% set forecast_input_data = "SYSTEM$QUERY_REFERENCE('" ~ forecast_input_data ~ "')" %}
    {% elif forecast_input_type in ["TABLE", "VIEW"] %}
        {% set forecast_input_data = "SYSTEM$REFERENCE('" ~ forecast_input_type ~ "', '" ~ forecast_input_data ~ "')" %}
    {% endif %}

    {% if forecast_series_colname is none %}
        {% set forecast_series_colname = "" %}
    {% else %}
        {% set forecast_series_colname = ",\n    SERIES_COLNAME => '" ~ forecast_series_colname ~ "'" %}
    {% endif %}

    {% set forecast =
        "\n    INPUT_DATA => " ~ forecast_input_data
        ~ ",\n    TIMESTAMP_COLNAME => '" ~ forecast_timestamp_colname
        ~ forecast_series_colname
    %}
{% endif %}

{% set load %}
EXECUTE IMMEDIATE $$
    BEGIN
        CALL {{ ml_model }}!FORECAST({{ forecast }});
        LET query_id := SQLID;
        CREATE OR REPLACE TABLE {{ this }} AS
            SELECT * FROM TABLE(RESULT_SCAN(:query_id));
    END;
$$;
{% endset %}

{{ return([train, load]) }}

{% endmacro %}