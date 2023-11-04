{% snapshot snp_tasty_bytes__customer_v1 %}
select
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['customer_id', 'hash(*)']
    ) }} as dbt_unique_key
from
    {{ ref('src_tasty_bytes__customer') }}
{% endsnapshot %}
