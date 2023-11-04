{% snapshot snp_tasty_bytes__city_v1 %}
select
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['city_id', 'hash(*)']
    ) }} as dbt_unique_key
from
    {{ ref('src_tasty_bytes__city') }}
{% endsnapshot %}
