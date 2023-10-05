with 

source as (

    select * from {{ source('forecast', 'sales_data') }}

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
