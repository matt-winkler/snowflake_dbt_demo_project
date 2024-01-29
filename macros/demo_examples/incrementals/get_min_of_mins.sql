{% macro get_min_of_mins(model_dates) %}
  
  {% set min_dates = [] %}
  {% if execute %}
    {% for (model_name, date_column) in model_dates %}
     
     {% set relation = ref(model_name) %}
     {% set sql %}
        select min({{date_column}}) from {{relation}};
     {% endset %}
     {% set min_date = dbt_utils.get_single_value(sql) %}
     {% do min_dates.append(min_date) %}

  {% endfor %}
  {% endif %}

  {% set min_dates = min_dates | sort %}
  {{return(min_dates)}}

{% endmacro %}