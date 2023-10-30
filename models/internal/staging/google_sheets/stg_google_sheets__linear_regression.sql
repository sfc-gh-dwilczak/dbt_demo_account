with 

source as ( select * from {{ ref('src_google_sheets__linear_regression') }}),

renamed as (

    select
        _row,
        x,
        y,
        _fivetran_synced

    from source

)

select * from renamed
