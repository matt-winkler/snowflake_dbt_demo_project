{%- macro generate_snapshot_schema_name(custom_schema_name, node) -%}
    
    {# illustration of setting environment variables to namespace a set dev schema for everyone #}
    {%- set default_dev_schema = env_var("DBT_DEV_TARGET_SCHEMA", 'dev') -%}

    {# all models go into one dev schema #}
    {%- if target.name == 'dev' -%}

        {{ default_dev_schema }}_{{ custom_schema_name }}
        
    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}