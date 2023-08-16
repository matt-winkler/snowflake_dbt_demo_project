{% macro get_incremental_model_warehouse() %}
  
  {% if is_incremental() %}
     
     {{return('big_boi')}}

  {% else %}
     
     {{return('little_boi')}}
    
  {% endif %}

{% endmacro %}