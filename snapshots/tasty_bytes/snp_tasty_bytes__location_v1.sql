{% snapshot snp_tasty_bytes__location_v1 %}
{{ generate_snapshot(
    model=ref('src_tasty_bytes__location'),
    keys=['location_id']
) }}
{% endsnapshot %}
