{% macro sorted_columns(model) %}
    {% set columns = [] %}

    {% for column in get_columns_in_relation(model) %}
        {% do columns.append(column.name) %}
    {% endfor %}
    
    {% do columns.sort() %}
    {{ return(columns) }}
{% endmacro %}
