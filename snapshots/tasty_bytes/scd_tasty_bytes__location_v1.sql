{% snapshot scd_tasty_bytes__location_v1 %}
select * from {{ ref('src_tasty_bytes__location') }}
{% endsnapshot %}
