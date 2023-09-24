with 

source as (

    select * from {{ source('forecast', 'sales_data') }}

),

renamed as (

    SELECT 
        date, sales
    FROM
        source
    WHERE
            store_id = 1
        AND item = 'jacket'
)

select * from renamed
