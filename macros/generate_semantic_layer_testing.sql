{{ generate_semantic_layer(
    name='query_history',
    references='src_snowflake__query_history'
    description='Query history of users in snowflake',
    primary_key='query_id',
    time='execution_time',
    granularity='day/week/month/year',
    dimensions=[
        'database_name',
        'account_name',
        'user_name',
        'warehouse_name'
    ],
    sums=['TOTAL_ELAPSED_TIME']
) }}