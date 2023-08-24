with 

source as (

    select * from {{ source('google_sheets', 'linear_regression') }}

),

renamed as (

    select
        _row::number,
        x::number,
        y::number,
        _fivetran_synced

    from source

)

select * from renamed
