{% snapshot snp_tasty_bytes__truck_v1 %}
select * from {{ ref('src_tasty_bytes__truck') }}
{% endsnapshot %}
