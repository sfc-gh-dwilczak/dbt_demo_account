with 

source as (

    select * from {{ source('hex', 'linear_regression_model_predictions') }}

),

renamed as (

    select
        "x" as x,
        "y" as y,
        predictions

    from source

)

select * from renamed
