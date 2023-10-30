{% snapshot scd_tasty_bytes__location %}
select * from {{ ref('src_tasty_bytes__location') }}
{% endsnapshot %}
