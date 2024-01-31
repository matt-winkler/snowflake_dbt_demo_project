{% macro generate_alias_name(custom_alias_name=none, node=none) -%}

    {%- if custom_alias_name -%}

        {{ custom_alias_name | trim }}
    
    {# generates the alias name for the root package #}
    {%- elif node.package_name == project_name -%}

        {%- if target.name == 'dev' %}
        
          {%- if node.version -%}

            {{ return(target.user ~ '_' ~ node.name ~ "_v" ~ (node.version | replace(".", "_"))) }}
           
          {%- else -%}
          
            {{ target.user }}_{{ node.name }}

          {%- endif -%}
        
        {%- else -%}
           
          {%- if node.version -%}

            {{ return(node.name ~ "_v" ~ (node.version | replace(".", "_"))) }}
           
          {%- else -%}
          
            {{ node.name }}

          {%- endif -%}
        
        {%- endif -%}
    
    {# for any imported package models #}
    {%- else -%}

        {%- if node.version -%}

          {{ return(node.name ~ "_v" ~ (node.version | replace(".", "_"))) }}
        
        {%- else -%}

          {{ node.name }}
        
        {%- endif -%}

    {%- endif -%}

{%- endmacro %}