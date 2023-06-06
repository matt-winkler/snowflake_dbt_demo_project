{% macro dynamic_select_columns(node) %}

  {% if execute %} 
     
     {%- call statement('get_column_values', fetch_result=true) %}
       select column_name as src_col, column_name as tgt_col
       from   {{ node.database }}.INFORMATION_SCHEMA.columns
       where  table_schema = upper('{{ node.schema }}')
       and    table_name = upper('{{ node.name }}')
       order by ordinal_position;
     {%- endcall -%}

     {%- set value_list = load_result('get_column_values') -%}

     {%- if value_list and value_list['data'] -%}
        {%- set values = value_list['data'] | map(attribute=0) | list %}
     {{ return(values) }}
     {%- endif -%}

  {% endif %}


{% endmacro %}