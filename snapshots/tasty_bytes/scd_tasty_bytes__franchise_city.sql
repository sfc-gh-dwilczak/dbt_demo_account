{% snapshot scd_tasty_bytes__franchise_city %}
select
    {{ dbt_utils.generate_surrogate_key(['franchise_id', 'city']) }} as franchise_city_id,
    franchise_id,
    city
from
    {{ ref('src_tasty_bytes__franchise') }}
{% endsnapshot %}
