{% snapshot scd_tasty_bytes__franchise %}
select
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['franchise_id', 'city']
    ) }} as franchise_city_id
from
    {{ ref('src_tasty_bytes__franchise') }}
{% endsnapshot %}
