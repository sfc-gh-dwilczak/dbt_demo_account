{% snapshot snp_google_sheets__linear_regression_v1 %}
{{ generate_snapshot_sql(
    model=ref('src_google_sheets__linear_regression'),
    keys=['_row']
) }}
{% endsnapshot %}
