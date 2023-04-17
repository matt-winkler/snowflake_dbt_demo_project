{%- macro generate_schema_name(custom_schema_name, node) -%}

    {%- if target.name != 'prod' -%}

        {{ target.schema }}_{{ custom_schema_name }}
        
    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}