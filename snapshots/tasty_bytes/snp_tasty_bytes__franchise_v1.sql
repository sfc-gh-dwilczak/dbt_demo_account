{% snapshot snp_tasty_bytes__franchise_v1 %}
{{ generate_snapshot_sql(
    model=ref('src_tasty_bytes__franchise'),
    keys=['franchise_id', 'city']
) }}
{% endsnapshot %}
