{{ generate_snapshot(
    model=ref('src_tasty_bytes__order_header'),
    keys=['order_id'],
    partition_by=['order_ts::date']
) }}
