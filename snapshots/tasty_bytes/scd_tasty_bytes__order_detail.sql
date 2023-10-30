{% snapshot scd_tasty_bytes__order_detail %}
select * from {{ ref('src_tasty_bytes__order_detail') }}
{% endsnapshot %}
