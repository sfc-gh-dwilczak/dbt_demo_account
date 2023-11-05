{% snapshot snp_snowflake_usage__warehouse_v1 %}
{{ generate_snapshot(
    model=ref('src_snowflake_usage__warehouse'),
    keys=['warehouse_id', 'start_time']
) }}
{% endsnapshot %}
