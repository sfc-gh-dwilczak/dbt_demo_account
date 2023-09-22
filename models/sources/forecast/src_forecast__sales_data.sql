with 

source as (

    select * from {{ source('forecast', 'sales_data') }}

),

renamed as (

    select
        store_id,
        item,
        date,
        sales,
        temperature,
        humidity,
        holiday

    from source

)

select * from renamed
