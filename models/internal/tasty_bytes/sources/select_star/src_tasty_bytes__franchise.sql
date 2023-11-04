select distinct * from {{ source('tasty_bytes', 'franchise') }}
