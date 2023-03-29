{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
    {% do return(adapter.dispatch('generate_alias_name', 'dbt')(custom_alias_name, node)) %}
{%- endmacro %}

{% macro default__generate_alias_name(custom_alias_name=none, node=none) -%}

    {%- if target.name == 'dev' -%}

        {%- if node.config.materialized != 'snapshot' -%}

          {%- if custom_alias_name is none -%}
            {# {{user-defined schema from their developer profile}}_{{+schema config from dbt_project.yml}}_{{model.sql file name}} #}
            {{ target.schema }}_{{node.config.schema}}_{{ node.name }}

          {%- else -%}

            {{ target.schema }}_{{node.config.schema}}_{{ (custom_alias_name | trim) }}

          {%- endif -%}

        {%- else -%}

           {%- if custom_alias_name is none -%}
          
             {{ target.schema }}_{{node.config.target_schema}}_{{ node.name }}
        
           {%- else -%}
             
             {{ target.schema }}_{{node.config.target_schema}}_{{ (custom_alias_name | trim) }}
           
           {%- endif -%}

        {%- endif -%}

    {%- else  -%}

        {%- if custom_alias_name is none -%}

            {{ node.name }}

        {%- else -%}

            {{ custom_alias_name | trim }}

        {%- endif -%}

    {%- endif -%}

{%- endmacro %}