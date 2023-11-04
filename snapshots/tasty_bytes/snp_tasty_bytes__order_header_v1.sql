{% snapshot snp_tasty_bytes__order_header_v1 %}
select
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['order_id', 'hash(*)']
    ) }} as dbt_unique_key
from
    {{ ref('src_tasty_bytes__order_header') }}
{% endsnapshot %}
