{% macro get_current_warehouse_size(snowflake_warehouse) %}
   {% set show_warehouse_size_sql %}
      show warehouses like '{{snowflake_warehouse}}';
   {% endset %}
   
   {% set result = run_query(show_warehouse_size_sql) %}
   {% set current_warehouse_size = result.columns[3].values()[0] %}

   {{return(current_warehouse_size)}}

{% endmacro %}

{% macro set_warehouse_size_from_explain_plan(compiled_code, default_warehouse_size='XSMALL') %}
   
   {% set warehouse_size = default_warehouse_size %}

   {% set warehouse_byte_ranges = [
      {'gb_lower': 0.0, 'gb_upper': 5.0, 'warehouse_size': 'XSMALL'},
      {'gb_lower': 5.0, 'gb_upper': 20.0, 'warehouse_size': 'SMALL'}, 
      {'gb_lower': 20.0, 'gb_upper': 50.0, 'warehouse_size': 'MEDIUM'}, 
      {'gb_lower': 50.0, 'gb_upper': 100.0, 'warehouse_size': 'LARGE'}, 
      {'gb_lower': 100.0, 'gb_upper': 250.0, 'warehouse_size': 'XLARGE'}, 
      {'gb_lower': 250.0, 'gb_upper': 500.0, 'warehouse_size': 'XXLARGE'}, 
      {'gb_lower': 500.0, 'gb_upper': 1000.0, 'warehouse_size': 'XXXLARGE'}, 
      {'gb_lower': 1000.0, 'gb_upper': 2000.0, 'warehouse_size': 'X4LARGE'}, 
      {'gb_lower': 2000.0, 'gb_upper': 5000.0, 'warehouse_size': 'X5LARGE'}, 
      {'gb_lower': 5000.0, 'gb_upper': None, 'warehouse_size': 'X6LARGE'}
   ]
   %}

   {% set explain_sql %}
      explain {{ compiled_code }};
   {% endset %}

   {% set explain_result = run_query(explain_sql) %}
   {% set bytes_assigned = explain_result.columns[9].values()[0] %}
   {% set gb_assigned = bytes_assigned / 1000000000.0 %}

   {{ log('gb_assigned for query: ' ~ gb_assigned) }}

   {% for range in warehouse_byte_ranges %}
     {# something here to handle the upper end of the range #}
     
     {% if (range['gb_lower'] <= gb_assigned) and (range['gb_upper'] > gb_assigned) %}
       {% set warehouse_size = range['warehouse_size'] %}
     {% endif %}
   
   {% endfor %}

   {{return(warehouse_size)}}

{% endmacro %}