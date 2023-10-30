select * from {{ source('forecast', 'sales_data') }}
