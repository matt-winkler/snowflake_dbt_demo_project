{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if node.package_name != project_name -%}
       
       {{ node.config.schema }}
   
    {%- elif target.name == 'dev'-%}
       
       {{ default_schema }}

    {%- else -%}
       
       {{ custom_schema_name }}

    {%- endif -%}

{%- endmacro %}