{% snapshot scd_tasty_bytes__order_header %}
select * from {{ ref('src_tasty_bytes__order_header') }}
{% endsnapshot %}
