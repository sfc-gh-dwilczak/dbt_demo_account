{% snapshot snp_tasty_bytes__order_detail_v1 %}
{{ generate_snapshot_sql(
    model=ref('src_tasty_bytes__order_detail'),
    keys=['order_detail_id']
) }}
{% endsnapshot %}
