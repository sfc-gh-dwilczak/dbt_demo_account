{%- set model = ref('src_tasty_bytes__order_detail') -%}

{%- set query = generate_snapshot_sql(
    model=model,
    keys=['order_detail_id']
) -%}

{{ snapshot_partitions(
    query=query,
    partition_by=['round(order_id / 1000)'],
    checksum=hash_agg(model),
    invalidate_hard_deletes=true
) }}
