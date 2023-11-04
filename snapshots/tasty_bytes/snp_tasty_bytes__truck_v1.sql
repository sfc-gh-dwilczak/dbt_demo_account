{% snapshot snp_tasty_bytes__truck_v1 %}
select
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['truck_id', 'hash(*)']
    ) }} as dbt_unique_key
from
    {{ ref('src_tasty_bytes__truck') }}
{% endsnapshot %}
