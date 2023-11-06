{% macro statistics(settings, pivot=false, columns=none) %}
    {% set models = {} %}
    {% for setting in settings.get("models", []) %}
        {% set name = setting.name %}
        {% if "model" not in models %}
            {% do models.update({
                "model": ref(setting.name),
                "statistics": setting.get("statistics", {}),
                "expressions": setting.get("expressions", []),
                "columns": setting.get("columns", [])
            }) %}
        {% endif %}
    {% endfor %}
    {% for setting in settings.get("sources", []) %}
        {% set source_name = setting.name %}
        {% for setting in setting.get("tables", []) %}
            {% if "model" not in models %}
                {% do models.update({
                    "model": source(source_name, setting.name),
                    "statistics": setting.get("statistics", {}),
                    "expressions": setting.get("expressions", []),
                    "columns": setting.get("columns", [])
                }) %}
            {% endif %}
        {% endfor %}
    {% endfor %}
    {% for setting in settings.get("seeds", []) %}
        {% if "model" not in models %}
            {% do models.update({
                "model": ref(setting.name),
                "statistics": setting.get("statistics", {}),
                "expressions": setting.get("expressions", []),
                "columns": setting.get("columns", [])
            }) %}
        {% endif %}
    {% endfor %}
    {% for setting in settings.get("snapshots", []) %}
        {% if "model" not in models %}
            {% do models.update({
                "model": ref(setting.name),
                "statistics": setting.get("statistics", {}),
                "expressions": setting.get("expressions", []),
                "columns": setting.get("columns", [])
            }) %}
        {% endif %}
    {% endfor %}
    {% set model = models.model %}
    {% set stats = models.statistics %}
    {% set expressions = models.expressions %}
    {% set column_settings = {} %}
    {% set stat_names = [
        "count", "count_distinct", "count_null", "count_zero", "count_negative", "count_empty_string",
        "percent_distinct", "percent_null", "percent_zero", "percent_negative", "percent_empty_string",
        "array_distinct", "min", "max", "avg", "stddev", "mode", "sum",
        "avg_distinct", "stddev_distinct", "sum_distinct"
    ] %}
    {% set defaults = {} %}
    {% for stat in stat_names %}
        {% do defaults.update({stat: stats.get(stat, false)}) %}
    {% endfor %}
    {% if columns is none %}
        {% set columns = get_columns_in_relation(model) %}
    {% endif %}
    {% for column in columns %}
        {% do column_settings.update({column.name: {}}) %}
        {% for stat in stat_names %}
            {% do column_settings[column.name].update({stat: defaults[stat]}) %}
        {% endfor %}
    {% endfor %}
    {% for column in models.columns %}
        {% if column.name in column_settings %}
            {% for stat in stat_names %}
                {% if column.get("statistics", {}).get(stat, false) %}
                    {% do column_settings[column.name].update({stat: true}) %}
                {% elif not column.get("statistics", {}).get(stat, true) %}
                    {% do column_settings[column.name].update({stat: false}) %}
                {% endif %}
            {% endfor %}
        {% endif %}
    {% endfor %}
    {% set _pivot = {"sql": "select"} %}
    {% set unpivot = {"sql": "select"} %}
    {% for column in columns %}
        {% set temp = {"sql": "'" ~ column.name ~ "' as column_name", "flag": false} %}
        {% set stat = column_settings.get(column.name, defaults).get("count", defaults.count) %}
        {% if stat %}
            {% do _pivot.update({"sql": _pivot.sql ~ '\n    count(' ~ column.name ~ ') as ' ~ adapter.quote(column.name ~ '__count') ~ ','}) %}
            {% do temp.update({"sql": temp.sql ~ ', ' ~ adapter.quote(column.name ~ '__count') ~ ' as count'}) %}
            {% do temp.update({"flag": true}) %}
        {% else %}
            {% do temp.update({"sql": temp.sql ~ ', null as count'}) %}
        {% endif %}
        {% set stat = column_settings.get(column.name, defaults).get("count_distinct", defaults.count_distinct) %}
        {% if stat %}
            {% do _pivot.update({"sql": _pivot.sql ~ '\n    count(distinct ' ~ column.name ~ ') as ' ~ adapter.quote(column.name ~ '__count_distinct') ~ ','}) %}
            {% do temp.update({"sql": temp.sql ~ ', ' ~ adapter.quote(column.name ~ '__count_distinct') ~ ' as count_distinct'})%}
            {% do temp.update({"flag": true}) %}
        {% else %}
            {% do temp.update({"sql": temp.sql ~ ', null as count_distinct'}) %}
        {% endif %}
        {% set stat = column_settings.get(column.name, defaults).get("count_null", defaults.count_null) %}
        {% if stat %}
            {% do _pivot.update({"sql": _pivot.sql ~ '\n    sum(case when ' ~ column.name ~ ' is null then 1 else 0 end) as ' ~ adapter.quote(column.name ~ '__count_null') ~ ','}) %}
            {% do temp.update({"sql": temp.sql ~ ', ' ~ adapter.quote(column.name ~ '__count_null') ~ ' as count_null'}) %}
            {% do temp.update({"flag": true}) %}
        {% else %}
            {% do temp.update({"sql": temp.sql ~ ', null as count_null'}) %}
        {% endif %}
        {% set stat = column_settings.get(column.name, defaults).get("count_zero", defaults.count_zero) %}
        {% if stat %}
            {% do _pivot.update({"sql": _pivot.sql ~ '\n    sum(case when ' ~ column.name ~ ' = 0 then 1 else 0 end) as ' ~ adapter.quote(column.name ~ '__count_zero') ~ ','}) %}
            {% do temp.update({"sql": temp.sql ~ ', ' ~ adapter.quote(column.name ~ '__count_zero') ~ ' as count_zero'}) %}
            {% do temp.update({"flag": true}) %}
        {% else %}
            {% do temp.update({"sql": temp.sql ~ ', null as count_zero'}) %}
        {% endif %}
        {% set stat = column_settings.get(column.name, defaults).get("count_negative", defaults.count_negative) %}
        {% if stat %}
            {% do _pivot.update({"sql": _pivot.sql ~ '\n    sum(case when ' ~ column.name ~ ' < 0 then 1 else 0 end) as ' ~ adapter.quote(column.name ~ '__count_negative') ~ ','}) %}
            {% do temp.update({"sql": temp.sql ~ ', ' ~ adapter.quote(column.name ~ '__count_negative') ~ ' as count_negative'}) %}
            {% do temp.update({"flag": true}) %}
        {% else %}
            {% do temp.update({"sql": temp.sql ~ ', null as count_negative'}) %}
        {% endif %}
        {% set stat = column_settings.get(column.name, defaults).get("count_empty_string", defaults.count_empty_string) %}
        {% if stat %}
            {% do _pivot.update({"sql": _pivot.sql ~ '\n    sum(case when ' ~ column.name ~ "::varchar = '' then 1 else 0 end) as " ~ adapter.quote(column.name ~ '__count_empty_string') ~ ','}) %}
            {% do temp.update({"sql": temp.sql ~ ', ' ~ adapter.quote(column.name ~ '__count_empty_string') ~ ' as count_empty_string'}) %}
            {% do temp.update({"flag": true}) %}
        {% else %}
            {% do temp.update({"sql": temp.sql ~ ', null as count_empty_string'}) %}
        {% endif %}
        {% set stat = column_settings.get(column.name, defaults).get("percent_distinct", defaults.percent_distinct) %}
        {% if stat %}
            {% do _pivot.update({"sql": _pivot.sql ~ '\n    count(distinct ' ~ column.name ~ ') / count(*) as ' ~ adapter.quote(column.name ~ '__percent_distinct') ~ ','}) %}
            {% do temp.update({"sql": temp.sql ~ ', ' ~ adapter.quote(column.name ~ '__percent_distinct') ~ ' as percent_distinct'}) %}
            {% do temp.update({"flag": true}) %}
        {% else %}
            {% do temp.update({"sql": temp.sql ~ ', null as percent_distinct'}) %}
        {% endif %}
        {% set stat = column_settings.get(column.name, defaults).get("percent_null", defaults.percent_null) %}
        {% if stat %}
            {% do _pivot.update({"sql": _pivot.sql ~ '\n    sum(case when ' ~ column.name ~ ' is null then 1 else 0 end) / count(*) as ' ~ adapter.quote(column.name ~ '__percent_null') ~ ','}) %}
            {% do temp.update({"sql": temp.sql ~ ', ' ~ adapter.quote(column.name ~ '__percent_null') ~ ' as percent_null'}) %}
            {% do temp.update({"flag": true}) %}
        {% else %}
            {% do temp.update({"sql": temp.sql ~ ', null as percent_null'}) %}
        {% endif %}
        {% set stat = column_settings.get(column.name, defaults).get("percent_zero", defaults.percent_zero) %}
        {% if stat %}
            {% do _pivot.update({"sql": _pivot.sql ~ '\n    sum(case when ' ~ column.name ~ ' = 0 then 1 else 0 end) / count(*) as ' ~ adapter.quote(column.name ~ '__percent_zero') ~ ','}) %}
            {% do temp.update({"sql": temp.sql ~ ', ' ~ adapter.quote(column.name ~ '__percent_zero') ~ ' as percent_zero'}) %}
            {% do temp.update({"flag": true}) %}
        {% else %}
            {% do temp.update({"sql": temp.sql ~ ', null as percent_zero'}) %}
        {% endif %}
        {% set stat = column_settings.get(column.name, defaults).get("percent_negative", defaults.percent_negative) %}
        {% if stat %}
            {% do _pivot.update({"sql": _pivot.sql ~ '\n    sum(case when ' ~ column.name ~ ' < 0 then 1 else 0 end) / count(*) as ' ~ adapter.quote(column.name ~ '__percent_negative') ~ ','}) %}
            {% do temp.update({"sql": temp.sql ~ ', ' ~ adapter.quote(column.name ~ '__percent_negative') ~ ' as percent_negative'}) %}
            {% do temp.update({"flag": true}) %}
        {% else %}
            {% do temp.update({"sql": temp.sql ~ ', null as percent_negative'}) %}
        {% endif %}
        {% set stat = column_settings.get(column.name, defaults).get("percent_empty_string", defaults.percent_empty_string) %}
        {% if stat %}
            {% do _pivot.update({"sql": _pivot.sql ~ '\n    sum(case when ' ~ column.name ~ "::varchar = '' then 1 else 0 end) / count(*) as " ~ adapter.quote(column.name ~ '__percent_empty_string') ~ ','}) %}
            {% do temp.update({"sql": temp.sql ~ ', ' ~ adapter.quote(column.name ~ '__percent_empty_string') ~ ' as percent_empty_string'}) %}
            {% do temp.update({"flag": true}) %}
        {% else %}
            {% do temp.update({"sql": temp.sql ~ ', null as percent_empty_string'}) %}
        {% endif %}
        {% set stat = column_settings.get(column.name, defaults).get("array_distinct", defaults.percent_empty_string) %}
        {% if stat %}
            {% do _pivot.update({"sql": _pivot.sql ~ '\n    array_agg(distinct ' ~ column.name ~ ') as ' ~ adapter.quote(column.name ~ '__array_distinct') ~ ','}) %}
            {% do temp.update({"sql": temp.sql ~ ', ' ~ adapter.quote(column.name ~ '__array_distinct') ~ ' as array_distinct'}) %}
            {% do temp.update({"flag": true}) %}
        {% else %}
            {% do temp.update({"sql": temp.sql ~ ', null as array_distinct'}) %}
        {% endif %}
        {% for stat_name in ["min", "max", "mode"] %}
            {% set stat = column_settings.get(column.name, defaults).get(stat_name, defaults[stat_name]) %}
            {% if stat %}
                {% do _pivot.update({"sql": _pivot.sql ~ '\n    ' ~ stat_name ~ '(' ~ column.name ~ ') as ' ~ adapter.quote(column.name ~ '__' ~ stat_name) ~ ','}) %}
                {% do temp.update({"sql": temp.sql ~ ', ' ~ adapter.quote(column.name ~ '__' ~ stat_name) ~ '::varchar as ' ~ adapter.quote(stat_name)}) %}
                {% do temp.update({"flag": true}) %}
            {% else %}
                {% do temp.update({"sql": temp.sql ~ ', null as ' ~ adapter.quote(stat_name)}) %}
            {% endif %}
        {% endfor %}
        {% for stat_name in ["avg", "stddev", "sum"] %}
            {% set stat = column_settings.get(column.name, defaults).get(stat_name, defaults[stat_name]) %}
            {% if stat %}
                {% do _pivot.update({"sql": _pivot.sql ~ '\n    ' ~ stat_name ~ '(' ~ column.name ~ ') as ' ~ adapter.quote(column.name ~ '__' ~ stat_name) ~ ','}) %}
                {% do temp.update({"sql": temp.sql ~ ', ' ~ adapter.quote(column.name ~ '__' ~ stat_name) ~ ' as ' ~ adapter.quote(stat_name)}) %}
                {% do temp.update({"flag": true}) %}
            {% else %}
                {% do temp.update({"sql": temp.sql ~ ', null as ' ~ adapter.quote(stat_name)}) %}
            {% endif %}
        {% endfor %}
        {% for stat_name in ["avg", "stddev", "sum"] %}
            {% set stat_distinct = stat_name ~ "_distinct" %}
            {% set stat = column_settings.get(column.name, defaults).get(stat_distinct, defaults[stat_distinct]) %}
            {% if stat %}
                {% do _pivot.update({"sql": _pivot.sql ~ '\n    ' ~ stat_name ~ '(distinct ' ~ column.name ~ ') as ' ~ adapter.quote(column.name ~ '__' ~ stat_distinct) ~ ','}) %}
                {% do temp.update({"sql": temp.sql ~ ', ' ~ adapter.quote(column.name ~ '__' ~ stat_distinct) ~ ' as ' ~ stat_distinct}) %}
                {% do temp.update({"flag": true}) %}
            {% else %}
                {% do temp.update({"sql": temp.sql ~ ', null as ' ~ stat_distinct}) %}
            {% endif %}
        {% endfor %}
        {% if temp.flag %}
            {% do unpivot.update({"sql": unpivot.sql ~ '\n    ' ~ temp.sql ~ '\nfrom\n    __pivoted\n\nunion all\n\nselect'}) %}
        {% endif %}
    {% endfor %}
    {% if pivot %}
        {% for expr in expressions %}
            {% do _pivot.update({"sql": _pivot.sql ~ '\n    ' ~ expr.expression ~ ' as ' ~ expr.name ~ ','}) %}
        {% endfor %}
        {% set sql = _pivot.sql[:-1] ~ "\nfrom\n    " ~ model %}
    {% else %}
        {% set sql = _pivot.sql[:-1] ~ "\nfrom\n    " ~ model %}
        {% set sql = "with\n    __pivoted as (\n        " ~ sql.replace("\n", "\n        ") ~ "\n    )\n\n" %}
        {% set sql = sql ~ unpivot.sql[:-19] %}
    {% endif %}
    {{ return(sql) }}
{% endmacro %}
