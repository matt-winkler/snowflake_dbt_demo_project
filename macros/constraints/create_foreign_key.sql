{% macro create_foreign_key(pk_table_relation, pk_column_names, fk_table_relation, fk_column_names) %}
  {{return(adapter.dispatch('create_foreign_key')(pk_table_relation, pk_column_names, fk_table_relation, fk_column_names))}}
{% endmacro %}

{%- macro snowflake__create_foreign_key(pk_table_relation, pk_column_names, fk_table_relation, fk_column_names, verify_permissions, quote_columns, constraint_name) -%}
  {%- set constraint_name = (constraint_name or fk_table_relation.split('.')[2] ~ "_" ~ fk_column_names|join('_') ~ "_FK") | upper -%}
  {%- set fk_columns_csv = get_quoted_column_csv(fk_column_names, quote_columns) -%}
  {%- set pk_columns_csv = get_quoted_column_csv(pk_column_names, quote_columns) -%}
  {#- Check that the PK table has a PK or UK -#}
  {%- if unique_constraint_exists(pk_table_relation, pk_column_names) -%}
    {#- Check if the table already has this foreign key -#}
    {%- if not foreign_key_exists(fk_table_relation, fk_column_names) -%}

      {%- set query -%}
       ALTER TABLE {{ fk_table_relation }} ADD CONSTRAINT {{ constraint_name }} FOREIGN KEY ( {{ fk_columns_csv }} ) REFERENCES {{ pk_table_relation }} ( {{ pk_columns_csv }} ) RELY
      {%- endset -%}
      {%- do log("Creating foreign key: " ~ constraint_name ~ " referencing " ~ pk_table_relation.split('.')[2] ~ " " ~ pk_column_names, info=true) -%}
      {%- do run_query(query) -%}

    {%- else -%}
        {%- do log("Skipping " ~ constraint_name ~ " because FK already exists: " ~ fk_table_relation ~ " " ~ fk_column_names, info=false) -%}
    {%- endif -%}

  {%- else -%}
      {%- do log("Skipping " ~ constraint_name ~ " because a PK/UK was not found on the PK table: " ~ pk_table_relation ~ " " ~ pk_column_names, info=true) -%}
  {%- endif -%}

{%- endmacro -%}