{% snapshot scd_tasty_bytes__franchise %}
select * from {{ ref('src_tasty_bytes__franchise') }}
{% endsnapshot %}
