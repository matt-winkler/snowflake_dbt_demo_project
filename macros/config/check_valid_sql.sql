
{% macro explain_sql(sql) %}
   
   {% set check_sql %}
      explain {{ sql }}
   {% endset %}

   {% do run_query(check_sql) %}
 
{% endmacro %}