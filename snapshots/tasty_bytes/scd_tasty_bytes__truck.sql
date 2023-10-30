{% snapshot scd_tasty_bytes__truck %}
select * from {{ ref('src_tasty_bytes__truck') }}
{% endsnapshot %}
