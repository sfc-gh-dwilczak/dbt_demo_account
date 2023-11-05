{% macro sorted_columns(model, quote=true) %}
    {% set columns = [] %}

    {% for column in get_columns_in_relation(model) %}
        {% if quote %}
            {% do columns.append('"' ~ column.name ~ '"') %}
        {% else %}
            {% do columns.append(column.name) %}
        {% endif %}
    {% endfor %}
    
    {% do columns.sort() %}
    {{ return(columns) }}
{% endmacro %}
