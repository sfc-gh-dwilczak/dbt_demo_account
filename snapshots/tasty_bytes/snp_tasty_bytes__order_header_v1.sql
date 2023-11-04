{% snapshot snp_tasty_bytes__order_header_v1 %}
{{ generate_snapshot_sql(
    model=ref('src_tasty_bytes__order_header'),
    keys=['order_id']
) }}
{% endsnapshot %}
