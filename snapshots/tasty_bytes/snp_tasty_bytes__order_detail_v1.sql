{% snapshot snp_tasty_bytes__order_detail_v1 %}
select * from {{ ref('src_tasty_bytes__order_detail') }}
{% endsnapshot %}
