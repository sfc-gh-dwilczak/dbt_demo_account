with 

source as (

    select * from {{ ref('src_forecast__sales_data') }}

),

renamed as (

    select 
        "DATE" as sold_on,
        sales
    from
        source
    where
        store_id = 1
        and item = 'jacket'
)

select * from renamed
