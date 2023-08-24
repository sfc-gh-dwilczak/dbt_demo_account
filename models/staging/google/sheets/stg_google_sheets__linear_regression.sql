with 

source as (

    select * from {{ source('google_sheets', 'linear_regression') }}

),

renamed as (

    select
        _row::INTEGER,
        x::INTEGER,
        y::INTEGER,
        _fivetran_synced

    from source

)

select * from renamed
