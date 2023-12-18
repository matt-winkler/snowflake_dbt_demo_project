{% macro choose_warehouse(default_warehouse, gb_assigned) %}
   
   {# set this in dbt_project.yml #}
   {% set warehouse_byte_ranges = [
      {'gb_lower': 0.0, 'gb_upper': 5.0, 'warehouse': 'MATT_W_DEV_XSMALL'},
      {'gb_lower': 5.0, 'gb_upper': 20.0, 'warehouse': 'MATT_W_DEV_SMALL'}, 
      {'gb_lower': 20.0, 'gb_upper': 50.0, 'warehouse': 'MATT_W_DEV_MEDIUM'}, 
   ]
   %}

   {% set max_warehouse = 'MATT_W_DEV_MEDIUM' %}

   {% for range in warehouse_byte_ranges %}
     
     {% if (range['gb_lower'] <= gb_assigned) and (range['gb_upper'] > gb_assigned) %}
       {% set warehouse = range['warehouse'] %}
     {% elif gb_assigned > warehouse_byte_ranges[-1]['warehouse'] %}
       {% set warehouse = max_warehouse %}
     {% endif %}

     {% if loop.last and warehouse == '' %}
       
     {% endif %}

   
   {% endfor %}

   {{return(warehouse_size)}}

{% endmacro %}