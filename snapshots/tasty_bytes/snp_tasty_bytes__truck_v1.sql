{% snapshot snp_tasty_bytes__truck_v1 %}
{{ generate_snapshot_sql(
    model=ref('src_tasty_bytes__truck'),
    keys=['truck_id']
) }}
{% endsnapshot %}
