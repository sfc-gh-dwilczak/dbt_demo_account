{% snapshot snp_tasty_bytes__franchise_v1 %}
select
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['franchise_id', 'city', 'hash(*)']
    ) }} as dbt_unique_key
from
    {{ ref('src_tasty_bytes__franchise') }}
{% endsnapshot %}
