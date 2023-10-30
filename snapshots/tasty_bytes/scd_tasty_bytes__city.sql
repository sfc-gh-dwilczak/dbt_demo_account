{% snapshot scd_tasty_bytes__city %}
select * from {{ ref('src_tasty_bytes__city') }}
{% endsnapshot %}
