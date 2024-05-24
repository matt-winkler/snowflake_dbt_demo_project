{% macro get_insert_overwrite_sql(target_relation, tmp_relation, dest_columns) %}
  {{ adapter.dispatch('insert_overwrite_get_sql', 'dbt')(target_relation, tmp_relation, dest_columns) }}
{% endmacro %}

{% macro snowflake__insert_overwrite_get_sql(target_relation, tmp_relation, dest_columns) %}
  
  {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute='name')) -%}
  {%- set sql_header = config.get('sql_header', none) -%}
  
  {%- set dml -%}
    {{ sql_header if sql_header is not none }}
    insert overwrite into {{ target_relation }} select {{ dest_cols_csv }} from {{ tmp_relation }}
  {%- endset -%}

  {% do return(snowflake_dml_explicit_transaction(dml)) %}
{% endmacro %}