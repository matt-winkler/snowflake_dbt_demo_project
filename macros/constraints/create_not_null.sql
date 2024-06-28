{% macro create_not_null(table_relation, column_names) %}
  {{return(adapter.dispatch('create_not_null')(table_relation, column_names))}}
{% endmacro %}

{%- macro snowflake__create_not_null(table_relation, column_names, quote_columns=false) -%}

  {%- set columns_list = get_quoted_column_list(column_names, quote_columns) -%}
  {%- set modify_statements= [] -%}

  {%- for column in columns_list -%}
    {%- set modify_statements = modify_statements.append( "COLUMN " ~ column ~ " SET NOT NULL" ) -%}
  {%- endfor -%}
    
  {%- set modify_statement_csv = modify_statements | join(", ") -%}
  {%- set query -%}
    ALTER TABLE {{ table_relation }} MODIFY {{ modify_statement_csv }};
  {%- endset -%}

  {%- do log("Creating not null constraint for: " ~ columns_to_change | join(", ") ~ " in " ~ table_relation, info=true) -%}
  {%- do run_query(query) -%}

{%- endmacro -%}