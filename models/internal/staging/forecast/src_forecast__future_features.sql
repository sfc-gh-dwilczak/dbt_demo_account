select * from {{ source('forecast', 'future_features') }}
