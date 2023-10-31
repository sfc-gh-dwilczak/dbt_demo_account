{% snapshot scd_snowflake__query_history %}
select 0 as dbt_check_col, * from {{ ref('src_snowflake__query_history') }}
{% endsnapshot %}
