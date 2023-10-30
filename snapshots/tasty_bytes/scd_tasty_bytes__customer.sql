{% snapshot scd_tasty_bytes__customer %}
select * from {{ ref('src_tasty_bytes__customer') }}
{% endsnapshot %}
