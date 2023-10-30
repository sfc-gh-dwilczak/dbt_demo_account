{% snapshot scd_tasty_bytes__franchise %}
select distinct * exclude city_name from {{ ref('src_tasty_bytes__franchise') }}
{% endsnapshot %}
