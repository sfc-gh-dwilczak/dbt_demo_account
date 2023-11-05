{% macro snapshot_with_partition_by(
    query,
    partition_by,
    checksum,
    unique_key='dbt_scd_uk',
    check_cols='all',
    invalidate_hard_deletes=false
) %}
    {% if not is_incremental() %}
        select
            *,
            md5(
                coalesce(cast({{ unique_key }} as varchar), '')
                || '|'
                || coalesce(
                        cast(to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as varchar),
                        ''
                    )
            ) as dbt_scd_id,
            to_timestamp(convert_timezone('UTC', current_timestamp())) as dbt_updated_at,
            to_timestamp(convert_timezone('UTC', current_timestamp())) as dbt_valid_from,
            nullif(
                to_timestamp_ntz(convert_timezone('UTC', current_timestamp())),
                to_timestamp_ntz(convert_timezone('UTC', current_timestamp()))
            ) as dbt_valid_to
        from (
            {{ query }}
        )
    {% else %}
        {% if check_cols == 'all' %}
            {% set check_cols = [] %}
            {%
                for column in get_columns_in_relation(this)
                if column.name.lower() not in [
                    'dbt_valid_from',
                    'dbt_valid_to',
                    'dbt_scd_id',
                    'dbt_updated_at'
                ]
            %}
                {% do check_cols.append(column.name) %}
            {% endfor %}
        {% endif %}
        with
            partition_source as (
                select
                    {%- set temp = {"index": 0} -%}
                    {%- for pb in partition_by %}
                    {{ pb }} as _dbt_partition_by_{{ temp.index }},
                    {%- do temp.update({"index": temp.index + 1}) -%}
                    {%- endfor %}
                    {{ checksum }} as _dbt_checksum
                from (
                    {{ query }}
                )
                group by
                    {%- set temp = {"index": 0} -%}
                    {%- for pb in partition_by -%} {%- if temp.index != 0 -%} , {%- endif %}
                    _dbt_partition_by_{{ temp.index }}
                    {%- do temp.update({"index": temp.index + 1}) -%}
                    {%- endfor %}
            ),
            
            partition_destination as (
                select
                    {%- set temp = {"index": 0} -%}
                    {%- for pb in partition_by %}
                    {{ pb }} as _dbt_partition_by_{{ temp.index }},
                    {%- do temp.update({"index": temp.index + 1}) -%}
                    {%- endfor %}
                    {{ checksum }} as _dbt_checksum
                from
                    {{ this }}
                where
                    dbt_valid_to is null
                group by
                    {%- set temp = {"index": 0} -%}
                    {%- for pb in partition_by -%} {%- if temp.index != 0 -%} , {%- endif %}
                    _dbt_partition_by_{{ temp.index }}
                    {%- do temp.update({"index": temp.index + 1}) -%}
                    {%- endfor %}
            ),
            
            partition_diff as (
                select
                    {%- set temp = {"index": 0} -%}
                    {%- for pb in partition_by %}
                    ifnull(
                        partition_source._dbt_partition_by_{{ temp.index }},
                        partition_destination._dbt_partition_by_{{ temp.index }}
                    ) as _dbt_partition_by_{{ temp.index }},
                    {%- do temp.update({"index": temp.index + 1}) -%}
                    {%- endfor %}
                    partition_source._dbt_checksum is not null as _dbt_matches_source,
                    partition_destination._dbt_checksum is not null as _dbt_matches_destination
                from
                    partition_source
                full outer join
                    partition_destination
                        on {% set temp = {"index": 0} -%}
                        {%- for pb in partition_by -%}
                        partition_source._dbt_partition_by_{{ temp.index }} is not distinct from partition_destination._dbt_partition_by_{{ temp.index }}
                        {% if not loop.last -%} and {% endif -%}
                        {%- do temp.update({"index": temp.index + 1}) -%}
                        {%- endfor %}
                where
                    partition_source._dbt_checksum is distinct from partition_destination._dbt_checksum
            ),
            
            source as (
                select
                    source.* exclude (
                        {%- set temp = {"index": 0} -%}
                        {%- for pb in partition_by -%} {%- if temp.index != 0 -%} , {%- endif %}
                        _dbt_partition_by_{{ temp.index }}
                        {%- do temp.update({"index": temp.index + 1}) -%}
                        {%- endfor %}
                    ),
                    partition_diff.* exclude (
                        {%- set temp = {"index": 0} -%}
                        {%- for pb in partition_by -%} {%- if temp.index != 0 -%} , {%- endif %}
                        _dbt_partition_by_{{ temp.index }}
                        {%- do temp.update({"index": temp.index + 1}) -%}
                        {%- endfor %}
                    )
                from (
                    select
                        *,
                        {{ unique_key }} as dbt_unique_key
                        {%- set temp = {"index": 0} -%}
                        {%- for pb in partition_by %},
                        {{ pb }} as _dbt_partition_by_{{ temp.index }}
                        {% do temp.update({"index": temp.index + 1}) %}
                        {%- endfor %}
                    from (
                        {{ query }}
                    )
                ) as source
                inner join
                    partition_diff
                        on {% set temp = {"index": 0} -%}
                        {%- for pb in partition_by -%}
                        source._dbt_partition_by_{{ temp.index }} is not distinct from partition_diff._dbt_partition_by_{{ temp.index }}
                        {% if not loop.last -%} and {% endif -%}
                        {%- do temp.update({"index": temp.index + 1}) -%}
                        {%- endfor %}
            ),
            
            destination as (
                select
                    destination.* exclude (
                        {%- set temp = {"index": 0} -%}
                        {%- for pb in partition_by -%} {%- if temp.index != 0 -%} , {%- endif %}
                        _dbt_partition_by_{{ temp.index }}
                        {%- do temp.update({"index": temp.index + 1}) -%}
                        {%- endfor %}
                    ),
                    partition_diff.* exclude (
                        {%- set temp = {"index": 0} -%}
                        {%- for pb in partition_by -%} {%- if temp.index != 0 -%} , {%- endif %}
                        _dbt_partition_by_{{ temp.index }}
                        {%- do temp.update({"index": temp.index + 1}) -%}
                        {%- endfor %}
                    )
                from (
                    select
                        *,
                        {{ unique_key }} as dbt_unique_key
                        {%- set temp = {"index": 0} -%}
                        {%- for pb in partition_by %},
                        {{ pb }} as _dbt_partition_by_{{ temp.index }}
                        {% do temp.update({"index": temp.index + 1}) %}
                        {%- endfor %}
                    from
                        {{ this }}
                    where
                        dbt_valid_to is null
                ) as destination
                inner join
                    partition_diff
                        on {% set temp = {"index": 0} -%}
                        {%- for pb in partition_by -%}
                        destination._dbt_partition_by_{{ temp.index }} is not distinct from partition_diff._dbt_partition_by_{{ temp.index }}
                        {% if not loop.last -%} and {% endif -%}
                        {%- do temp.update({"index": temp.index + 1}) -%}
                        {%- endfor %}
            ),
            
            partition_inserts as (
                select
                    * exclude (
                        dbt_unique_key,
                        _dbt_matches_destination,
                        _dbt_matches_source
                    ),
                    md5(
                        coalesce(cast(dbt_unique_key as varchar), '')
                        || '|'
                        || coalesce(
                                cast(to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as varchar),
                                ''
                            )
                    ) as dbt_scd_id,
                    to_timestamp(convert_timezone('UTC', current_timestamp())) as dbt_updated_at,
                    to_timestamp(convert_timezone('UTC', current_timestamp())) as dbt_valid_from,
                    nullif(
                        to_timestamp_ntz(convert_timezone('UTC', current_timestamp())),
                        to_timestamp_ntz(convert_timezone('UTC', current_timestamp()))
                    ) as dbt_valid_to
                from
                    source
                where
                    not _dbt_matches_destination
            ),
            
            partition_deletes as (
                select
                    * exclude (
                        dbt_scd_id,
                        dbt_updated_at,
                        dbt_valid_from,
                        dbt_valid_to,
                        dbt_unique_key,
                        _dbt_matches_destination,
                        _dbt_matches_source
                    ),
                    dbt_scd_id,
                    to_timestamp(convert_timezone('UTC', current_timestamp())) as dbt_updated_at,
                    dbt_valid_from,
                    to_timestamp(convert_timezone('UTC', current_timestamp())) as dbt_valid_to
                from
                    destination
                where
                    not _dbt_matches_source
            ),
            
            filtered_source as (
                select
                    * exclude (
                        _dbt_matches_destination,
                        _dbt_matches_source
                    )
                from
                    source
                where
                    _dbt_matches_destination
            ),
            
            filtered_destination as (
                select
                    * exclude (
                        _dbt_matches_destination,
                        _dbt_matches_source
                    )
                from
                    destination
                where
                    _dbt_matches_source
            ),
            
            filtered_diff as (
                select
                    dbt_unique_key
                from
                    filtered_source
                full outer join
                    filtered_destination
                        using(dbt_unique_key)
                where
                    {%- for col in check_cols %}
                    filtered_source.{{ col }} is distinct from filtered_destination.{{ col }}
                    {% if not loop.last -%} or {% endif -%}
                    {% endfor %}
            ),
            
            filtered_inserts as (
                select
                    filtered_source.* exclude dbt_unique_key,
                    md5(
                        coalesce(cast(dbt_unique_key as varchar), '')
                        || '|'
                        || coalesce(
                                cast(to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as varchar),
                                ''
                            )
                    ) as dbt_scd_id,
                    to_timestamp(convert_timezone('UTC', current_timestamp())) as dbt_updated_at,
                    to_timestamp(convert_timezone('UTC', current_timestamp())) as dbt_valid_from,
                    nullif(
                        to_timestamp_ntz(convert_timezone('UTC', current_timestamp())),
                        to_timestamp_ntz(convert_timezone('UTC', current_timestamp()))
                    ) as dbt_valid_to
                from
                    filtered_source
                inner join
                    filtered_diff
                        using(dbt_unique_key)
            ),
            
            filtered_deletes as (
                select
                    filtered_destination.* exclude (
                        dbt_scd_id,
                        dbt_updated_at,
                        dbt_valid_from,
                        dbt_valid_to,
                        dbt_unique_key
                    ),
                    filtered_destination.dbt_scd_id,
                    to_timestamp(convert_timezone('UTC', current_timestamp())) as dbt_updated_at,
                    filtered_destination.dbt_valid_from,
                    to_timestamp(convert_timezone('UTC', current_timestamp())) as dbt_valid_to
                from
                    filtered_destination
                inner join
                    filtered_diff
                        using(dbt_unique_key)
            )
        
        select * from partition_inserts
        union all
        select * from filtered_inserts
        {%- if invalidate_hard_deletes %}
        union all
        select * from partition_deletes
        union all
        select * from filtered_deletes
        {%- endif -%}
    {%- endif -%}
{% endmacro %}
