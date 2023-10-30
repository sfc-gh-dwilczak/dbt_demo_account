{% snapshot scd_snowflake__query_history %}
select * from {{ ref('src_snowflake__query_history') }}
{% endsnapshot %}
