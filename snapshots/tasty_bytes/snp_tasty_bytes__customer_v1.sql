{% snapshot snp_tasty_bytes__customer_v1 %}
{{ generate_snapshot(
    model=ref('src_tasty_bytes__customer'),
    keys=['customer_id']
) }}
{% endsnapshot %}
