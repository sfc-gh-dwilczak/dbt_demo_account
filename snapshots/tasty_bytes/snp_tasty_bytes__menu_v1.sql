{% snapshot snp_tasty_bytes__menu_v1 %}
select * from {{ ref('src_tasty_bytes__menu') }}
{% endsnapshot %}
