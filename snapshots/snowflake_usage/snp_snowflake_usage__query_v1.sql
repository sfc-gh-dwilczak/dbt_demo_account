{% snapshot snp_snowflake_usage__query_v1 %}
{{ generate_snapshot(
    model=ref('src_snowflake_usage__query'),
    keys=['query_id']
) }}
{% endsnapshot %}
