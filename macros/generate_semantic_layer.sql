{% macro generate_semantic_layer(
    name,
    references,
    description=none,
    primary_key=none,
    time=none,
    granularity='day',
    dimensions=[],
    count=false,
    maxs=[],
    mins=[],
    sums=[]
) %}
semantic_layer:
  - name: {{ name}}
    {%- if description is not none %}
    description: {{ description }}
    {%- endif %}
    model: ref('{{ references }}')
    {%- if time is not none %}
    defaults:
      agg_time_dimension: {{ time }}
    {%- endif %}
    {%- if primary_key is not none %}
    entities:
      - name: {{ primary_key }}
        type: primary
    {%- else %}
    primary_entity: {{ name }}
    {%- endif %}
    {%- if time is not none or dimensions != [] %}
    dimensions:
      {%- if time is not none %}
      - name: {{ time }}
        type: time
        type_params:
          time_granularity: {{ granularity }}
      {%- endif %}
      {%- for dimension in dimensions %}
      - name: {{ dimension }}
        type: categorical
      {%- endfor %}
    {%- endif %}
    {%- if
      count
      or maxs != []
      or mins != []
      or sums != []
    %}
    measures:
      {%- if count %}
      - name: total_{{ name }}
        expr: 1
        agg: sum
      {%- endif %}
      {%- for measure in maxs %}
      - name: {{ measure }}
        expr: {{ measure }}
        agg: max
      {%- endfor %}
      {%- for measure in mins %}
      - name: {{ measure }}
        expr: {{ measure }}
        agg: min
      {%- endfor %}
      {%- for measure in sums %}
      - name: {{ measure }}
        expr: {{ measure }}
        agg: sum
      {%- endfor %}
    {%- endif %}

metrics:
  {%- if count %}
  - name: total_{{ name }}
    label: totL_{{ name }}
    type: simple
    type_params:
      measure: total_{{ name }}
  {%- endif %}
  {%- for measure in (maxs + mins + sums) %}
  - name: {{ measure }}
    description: {{ measure }}
    type: simple
    type_params:
      measure: {{ measure }}
  {%- endfor %}
{% endmacro %} 