with 

source as (

    select * from {{ source('forecast', 'future_features') }}

),

renamed as (

    select
        store_id,
        item,
        date,
        temperature,
        humidity,
        holiday

    from source

)

select * from renamed
