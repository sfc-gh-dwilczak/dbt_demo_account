{% macro generate_snapshot(
    model,
    keys,
    unique_key_alias='dbt_scd_uk',
    partition_by=none,
    invalidate_hard_deletes=true
) %}
    {% set query = snapshot_with_checksum_key(model, keys, unique_key_alias) %}
    {% if partition_by is none %}
        {{ return(query) }}
    {% else %}
        {{ return(snapshot_with_partition_by(
            query=query,
            unique_key=unique_key_alias,
            partition_by=partition_by,
            checksum=hash_agg(model),
            invalidate_hard_deletes=invalidate_hard_deletes
        )) }}
    {% endif %}
{% endmacro %}
