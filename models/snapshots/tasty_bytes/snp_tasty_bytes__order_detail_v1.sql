{{ generate_snapshot(
    model=ref('src_tasty_bytes__order_detail'),
    keys=['order_detail_id'],
    partition_by=['round(order_id / 1000)']
) }}
