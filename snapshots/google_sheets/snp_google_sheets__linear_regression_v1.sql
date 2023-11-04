{% snapshot snp_google_sheets__linear_regression_v1 %}
select
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['x', 'hash(*)']
    ) }} as dbt_unique_key
from
    {{ ref('src_google_sheets__linear_regression') }}
{% endsnapshot %}
