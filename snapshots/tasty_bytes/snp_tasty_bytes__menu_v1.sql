{% snapshot snp_tasty_bytes__menu_v1 %}
select
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['menu_item_id', 'hash(*)']
    ) }} as dbt_unique_key
from
    {{ ref('src_tasty_bytes__menu') }}
{% endsnapshot %}
