with 

source as (

    select * from {{ ref('src_forecast__future_features') }}

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
