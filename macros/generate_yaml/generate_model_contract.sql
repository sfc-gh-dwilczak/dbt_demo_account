{% macro generate_model_contract(model) %}
    {{ return(
        codegen.generate_model_yaml(model_names=[model])
        | replace('\n        description: ""', '')
        | replace(
                '    description: ""',
                '    config:\n'
                ~ '      contract:\n'
                ~ '        enforced: true\n'
                ~ '    '
            )
    ) }}
{% endmacro %}
