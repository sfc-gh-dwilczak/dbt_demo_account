with 

source as (

    select * from {{ ref('src_tasty_bytes__truck') }}

),

renamed as (

    select
        truck_id,
        menu_type_id,
        primary_city,
        region as region_name,
        iso_region,
        country as country_name,
        iso_country_code,
        franchise_flag,
        "YEAR" as truck_year,
        make as truck_make,
        model as truck_model,
        ev_flag,
        franchise_id,
        truck_opening_date

    from source

)

select * from renamed
