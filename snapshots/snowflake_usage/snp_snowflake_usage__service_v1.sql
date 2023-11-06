{% snapshot snp_snowflake_usage__service_v1 %}
{{ generate_snapshot(
    model=ref('src_snowflake_usage__service'),
    keys=['service_type', 'entity_id', 'start_time']
) }}
{% endsnapshot %}
