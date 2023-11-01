{% snapshot snp_tasty_bytes__city_v1 %}
select * from {{ ref('src_tasty_bytes__city') }}
{% endsnapshot %}
