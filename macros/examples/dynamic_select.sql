{% macro dynamic_select_columns(database_name, schema_name, table_name) %}

  {% if execute %} 
     
     {%- set sql -%}
       select column_name as src_col, column_name as tgt_col
       from   {{ database_name }}.INFORMATION_SCHEMA.columns
       where  table_schema = upper('{{ schema_name }}')
       and    table_name = upper('{{ table_name }}')
       order by ordinal_position;
     {%- endset -%}

     {%- set columns = run_query(sql) -%}

     {{ return(columns) }}

  {% endif %}


{% endmacro %}