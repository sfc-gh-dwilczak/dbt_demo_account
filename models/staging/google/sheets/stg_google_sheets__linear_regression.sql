with 

source as ( select * from {{ source('google_sheets', 'linear_regression') }}),

renamed as (

    select
        _row::INTEGER    as _row,
        x::INTEGER       as x,
        y::INTEGER       as y,
        _fivetran_synced as _fivetran_synced

    from source

)

select * from renamed
