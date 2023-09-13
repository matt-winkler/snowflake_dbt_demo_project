{% macro json_to_model_sql(
    source_or_model_name,
    json_column,
    is_source, 
    source_name,
    flatten_arrays = true,
    explode_mappings = false
    ) 
%}
    
    {% set columns = generate_column_names(source_or_model_name, json_column, is_source, source_name ) %}
    {% set simple_types = ['VARCHAR'] %}
    {% set simple_columns = [] %}
    {% set array_columns = [] %}
    {% set mapping_columns = [] %}

    {% for column in columns %}

      {% if column.COLUMN_TYPE in simple_types %}
        {% do simple_columns.append(column) %}
      
      {% elif column.COLUMN_TYPE == 'ARRAY' %}
        {{array_columns.append(column)}}
      
      {% elif column.COLUMN_TYPE == 'MAPPING' %}
        {{mapping_columns.append(column)}}
      
      {% else %}
        {% set msg %}
          "column name " ~ column.COLUMN_NAME ~ " with type " ~ column.COLUMN_TYPE ~ " is not a recognized data type."
        {% endset %}
        {{log(msg)}}
      
      {% endif %}
    {% endfor %}

    {% set n_array_columns = array_columns|length %}
    {% set n_mapping_columns = mapping_columns|length %}
    {% set n_nested_columns = n_array_columns + n_mapping_columns %}

    {% set array_flatten_sqls = [] %}
    
    with base as (
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
    )
    {% if n_array_columns > 0 and flatten_arrays %}
    , 
        {% for column in array_columns %}
            {% set cte_name = column.COLUMN_NAME ~ '_flattened'%}
            {% if loop.first %}
              {{cte_name}} as (
                select {{ simple_columns|join(', ') }}
                       flattened_data.value as {{column.COLUMN_NAME}}
                from   base a,
                table(flatten(input => {{column.COLUMN_NAME}})) as flattened_data
              )

            {% else %}

              {{cte_name}} as (
                select a.*,
                       flattened_data.value as {{column.COLUMN_NAME}}
                from   {{ array_flatten_sqls[-1] }} a,
                table(flatten(input => {{column.COLUMN_NAME}})) as flattened_data
              )
               
            {% endif %}
            
            {{ array_flatten_sqls.append(cte_name) }}
            
            {% if not loop.last %}
            , 
            {% endif %}
        {%- endfor -%}
    
    select * from experiment_mappings_flattened 

    {% else %}
    select * from base
    {% endif %}

{% endmacro %}

{# generates a list of columns names from a json column #}
{% macro generate_column_names(source_or_model_name, json_column, is_source = false, source_name = '', row_limit = 10000) %}

    {% set json_parse_query %}
      select 
        json_data.key as COLUMN_NAME,
        typeof(json_data.value) as COLUMN_TYPE
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