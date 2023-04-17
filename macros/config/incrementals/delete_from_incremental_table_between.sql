{% macro delete_from_incremental_table_between(table, date_column) %}
  
  {% set sql %}
    delete from {{table}} where {{date_column}} between '{{var('begin_date', '1992-01-01')}}' and '{{var('end_date', '1998-08-02')}}'; 
  {% endset %}

  {% do log(sql, info=true)%}
  
  {% do run_query(sql) %}

{% endmacro %}