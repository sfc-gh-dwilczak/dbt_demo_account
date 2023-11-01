{% snapshot snp_tasty_bytes__order_header_v1 %}
select * from {{ ref('src_tasty_bytes__order_header') }}
{% endsnapshot %}
