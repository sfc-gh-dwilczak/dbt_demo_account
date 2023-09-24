    /* 
    < 5 Weekday
    5 = Saturday
    6 = Sunday 
    
    sudo:
        if saturday:
            train model
            predict and load data
        else:
            predict and load data
*/

{% if run_started_at.weekday() == 5 %}
    {% set train %}
        CREATE OR REPLACE SNOWFLAKE.ML.FORECAST model2(
        INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{{ ref('src_forecast__sales_data') }}'),
        TIMESTAMP_COLNAME => 'date',
        TARGET_COLNAME => 'sales');
    {% endset %}

    {% set load %}
        EXECUTE IMMEDIATE $$
            BEGIN
                CALL model2!FORECAST(FORECASTING_PERIODS => 3);
                LET query_id := SQLID;
                CREATE OR REPLACE TABLE {{this}} AS
                     SELECT * FROM TABLE(RESULT_SCAN(:query_id));
            END;
        $$;
    {% endset %}
    {% set pre_hook = [train,load] %}
{% else %}
    {% set load %}
        EXECUTE IMMEDIATE $$
            BEGIN
                CALL model2!FORECAST(FORECASTING_PERIODS => 3);
                LET query_id := SQLID;
                CREATE OR REPLACE TABLE {{this}} AS
                    SELECT * FROM TABLE(RESULT_SCAN(:query_id));
            END;
        $$;
    {% endset %}
    {% set pre_hook = [load] %}
{% endif %}

{{ config(pre_hook=pre_hook) }}

-- Normal staging model code.
with
    source as (

        select * from {{ this }}

    ),

    renamed as (

        select
            ts,
            forecast,
            lower_bound,
            upper_bound

        from source

    )

select * from renamed
