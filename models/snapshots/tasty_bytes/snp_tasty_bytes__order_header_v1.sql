{%- set model = ref('src_tasty_bytes__order_header') -%}

{%- set query = generate_snapshot_sql(
    model=model,
    keys=['order_id']
) -%}

{{ snapshot_partitions(
    query=query,
    partition_by=['order_ts::date'],
    checksum=hash_agg(model),
    invalidate_hard_deletes=true
) }}
