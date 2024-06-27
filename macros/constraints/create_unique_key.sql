{% macro create_unique_key(table_relation, column_names) %}
  {{return(adapter.dispatch('create_unique_key')(table_relation, column_names))}}
{% endmacro %}

{%- macro snowflake__create_unique_key(table_relation, column_names, quote_columns=false, constraint_name=none, lookup_cache=none) -%}
{%- set constraint_name = (constraint_name or table_relation.split('.')[2] ~ "_" ~ column_names|join('_') ~ "_UK") | upper -%}
{%- set columns_csv = get_quoted_column_csv(column_names, quote_columns) -%}

{#- Check that the table does not already have this PK/UK -#}
{%- if not unique_constraint_exists(table_relation, column_names) -%}

  {%- set query -%}
    ALTER TABLE {{ table_relation }} ADD CONSTRAINT {{ constraint_name }} UNIQUE ( {{ columns_csv }} ) RELY
  {%- endset -%}
  {%- do log("Creating unique key: " ~ constraint_name, info=true) -%}
  {%- do run_query(query) -%}

{%- else -%}
  {%- do log("Skipping " ~ constraint_name ~ " because PK/UK already exists: " ~ table_relation ~ " " ~ column_names, info=false) -%}
{%- endif -%}

{%- endmacro -%}