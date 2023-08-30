{% macro json_to_model_sql( source_or_model_name, json_column, is_source = false, source_name = '' ) %}
    {% set columns = generate_column_names( source_or_model_name, json_column, is_source, source_name ) %}
    
    select 
    {% for column in columns %}
        {{ json_column }}:{{ column.COLUMN_NAME }} as {{ column.COLUMN_NAME }}
        {%- if not loop.last -%},{%- endif -%}
    {% endfor %}
    from
    {% if not is_source %}
        {{ ref(source_or_model_name) }}
    {% else %}
        {{ source(source_name, source_or_model_name) }}    
    {% endif %}

{% endmacro %}

{# generates a list of columns names from a json column #}
{% macro generate_column_names(source_or_model_name, json_column, is_source = false, source_name = '', row_limit = 10000) %}

    {% set json_parse_query %}
      select 
        json_data.key as COLUMN_NAME
      from (
        select * from 
        {% if not is_source %}
            {{ ref(source_or_model_name) }}
        {% else %}
            {{ source(source_name, source_or_model_name) }}    
        {% endif %} 
        limit {{ row_limit }}) json_table
         ,
         lateral flatten( input => json_table.{{ json_column }} ) json_data
    
    {% endset %}

    {% set results = run_query(json_parse_query) %}
      
      {% if execute %}
        {% set res_list = results.rows %}
      {% else %}
        {% set res_list = [] %}
      {% endif %}
    
    {{ return(res_list) }}

{% endmacro %}