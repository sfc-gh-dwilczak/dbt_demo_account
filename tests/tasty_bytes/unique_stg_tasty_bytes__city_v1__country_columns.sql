select
    count(*) as total_records,
    count(distinct country_id) as total_ids,
    count(distinct country_name) as total_names,
    count(distinct country_iso_code) as total_iso_codes
from (
    select distinct
        country_id,
        country_name,
        country_iso_code,
        currency_iso_code
    from
        {{ ref('stg_tasty_bytes__city', v=1) }}
)
having
    count(*) != count(distinct country_id)
    or count(*) != count(distinct country_name)
    or count(*) != count(distinct country_iso_code)
