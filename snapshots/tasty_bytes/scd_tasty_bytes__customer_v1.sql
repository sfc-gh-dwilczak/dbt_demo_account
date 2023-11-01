{% snapshot scd_tasty_bytes__customer_v1 %}
select * from {{ ref('src_tasty_bytes__customer') }}
{% endsnapshot %}
