{#
On the DAG, the model will reference the
data that the ML model is using to train on.
#}
{%- set train -%}
    CREATE OR REPLACE SNOWFLAKE.ML.FORECAST model2(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{{ ref('src_forecast__sales_data') }}'),
    TIMESTAMP_COLNAME => 'date',
    TARGET_COLNAME => 'sales');
{%- endset -%}

{#
The ML model will be used to forecast data, which
is saved in a table replacing this staging model.
#}
{%- set load -%}
    EXECUTE IMMEDIATE $$
        BEGIN
            CALL model2!FORECAST(FORECASTING_PERIODS => 3);
            LET query_id := SQLID;
            CREATE OR REPLACE TABLE {{ this }} AS
                    SELECT * FROM TABLE(RESULT_SCAN(:query_id));
        END;
    $$;
{%- endset -%}

{# The ML model is only re-trained on Saturday. #}
{%- set is_saturday = run_started_at.weekday() == 5 -%}

{%- if is_saturday -%}
    {%- set post_hook = [train, load] -%}
{%- else -%}
    {%- set post_hook = [load] -%}
{%- endif -%}

{{ config(post_hook=post_hook) }}

-- Define table structure for DBT developers to see and DBT auto-complete.
select
    null::timestamp     as ts,
    null::float         as forecast,
    null::float         as lower_bound,
    null::float         as upper_bound
-- The table shouldn't actually have any records.
where
    false