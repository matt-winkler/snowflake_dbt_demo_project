{% macro explain_query(compiled_sql) %}
  
  {% set explain_sql %}
     explain {{explain_sql}};
  {% endset %}

   {% set explain_sql_result = run_query(explain_sql) %}
   {% set bytes_assigned = explain_sql_result.columns[9].values()[0] %}
   {% set gb_assigned = bytes_assigned / 1000000000.0 %}

  {{return({'bytes_assigned': bytes_assigned, 'gb_assigned': gb_assigned})}}

{% endmacro %}
