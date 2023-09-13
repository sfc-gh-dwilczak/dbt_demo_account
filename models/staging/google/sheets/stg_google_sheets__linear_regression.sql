with 

source as ( select * from {{ source('google_sheets', 'linear_regression') }}),

renamed as (

    select
        _row,
        x,
        y,
        _fivetran_synced as data 

    from source

)

select * from renamed
