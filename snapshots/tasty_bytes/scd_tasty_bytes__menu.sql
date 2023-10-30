{% snapshot scd_tasty_bytes__menu %}
select * from {{ ref('src_tasty_bytes__menu') }}
{% endsnapshot %}
