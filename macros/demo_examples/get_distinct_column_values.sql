{% macro get_distinct_column_values(table_name, column_name) %}
   {% if execute %}
     {% set sql %}
        select {{column_name}} from {{ table_name }} group by 1 
     {% endset %}
    
   {% set result = run_query(sql) %}

   {{ return(result.columns[0].values()) }}
    
   {% endif %}

{% endmacro %}