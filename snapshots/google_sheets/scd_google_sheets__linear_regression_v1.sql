{% snapshot scd_google_sheets__linear_regression_v1 %}
select * from {{ ref('src_google_sheets__linear_regression') }}
{% endsnapshot %}
