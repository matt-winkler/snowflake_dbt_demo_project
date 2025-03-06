{% macro my_store_test_results(results) %}

  {%- set central_tbl -%} {{ target.database }}.{{ target.schema }}.test_results_central {%- endset -%}
  {%- set history_tbl -%} {{ target.database }}.{{ target.schema }}.test_results_history {%- endset -%}
  
  {{ log("Centralizing test data in " + central_tbl, info = true) if execute }}
  
  {% set test_results = [] %}
  {% for result in results %}
    {% if result.node.resource_type == 'test' %}
      {% do test_results.append(result) %}
    {% endif %}
  {% endfor %}
  {% set test_results_len = test_results|length %}

  {{ log(test_results_len)}}
  
  {% if test_results_len > 0 %}
    create or replace table {{ central_tbl }} as (
  
    {% for result in test_results %}

    {% set test_name='' %}
    {% set test_type='' %}

    {% if result.node.test_metadata is defined %}
      {% set test_name = result.node.test_metadata.name %}
      {% set test_type='generic' %}
    {% elif result.node.name is defined %}
      {% set test_name = result.node.name %}
      {% set test_type='singular' %}
    {% endif %}
    
    select
      '{{ test_name }}'::text as test_name,
      '{{ result.node.name }}'::text as test_name_long,
      '{{ test_type}}'::text as test_type,
      '{{ dbt_snowflake_query_tags.process_refs(result.node.refs) }}'::text as model_refs,
      '{{ dbt_snowflake_query_tags.process_refs(result.node.sources, is_src=true) }}'::text as source_refs,
      '{{ result.node.config.severity }}'::text as test_severity_config,
      '{{ result.execution_time }}'::text as execution_time_seconds,
      '{{ result.status }}'::text as test_result,
      '{{ result.node.original_file_path }}'::text as file_test_defined,
      current_timestamp as _timestamp
    
    {{ "union all" if not loop.last }}
  
  {% endfor %}
  );

  {% if target.name != 'default' %}
      create table if not exists {{ history_tbl }} as (
        select 
          {{ dbt_utils.generate_surrogate_key(["test_name", "test_result", "_timestamp"]) }} as sk_id, 
          * 
        from {{ central_tbl }}
        where false
      );

    insert into {{ history_tbl }} 
      select 
       {{ dbt_utils.generate_surrogate_key(["test_name", "test_result", "_timestamp"]) }} as sk_id, 
       * 
      from {{ central_tbl }}
    ;
  {% endif %}

  {% endif %}

{% endmacro %}


/*
  return a comma delimited string of the models or sources were related to the test.
    e.g. dim_customers,fct_orders

  behaviour changes slightly with the is_src flag because:
    - models come through as [['model'], ['model_b']]
    - srcs come through as [['source','table'], ['source_b','table_b']]
*/

{% macro process_refs( ref_list, is_src=false ) %}
  {% set refs = [] %}

  {% if ref_list is defined and ref_list|length > 0 %}
      {% for ref in ref_list %}
        {% if is_src %}
          {{ refs.append(ref|join('.')) }}
        {% else %}
          {{ refs.append(ref[0]) }}
        {% endif %} 
      {% endfor %}

      {{ return(refs|join(',')) }}
  {% else %}
      {{ return('') }}
  {% endif %}
{% endmacro %}
