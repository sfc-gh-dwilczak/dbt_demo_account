{% snapshot snp_snowflake_usage__storage_v1 %}
{{ generate_snapshot(
    model=ref('src_snowflake_usage__storage'),
    keys=['usage_date']
) }}
{% endsnapshot %}
