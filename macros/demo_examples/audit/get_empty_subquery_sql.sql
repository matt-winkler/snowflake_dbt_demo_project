{% macro get_empty_subquery_sql(select_sql) -%}
  {{ return(adapter.dispatch('get_empty_subquery_sql', 'dbt')(select_sql)) }}
{% endmacro %}

{#
  Builds a query that results in the same schema as the given select_sql statement, without necessitating a data scan.
  Useful for running a query in a 'pre-flight' context, such as model contract enforcement (assert_columns_equivalent macro).
#}
{% macro default__get_empty_subquery_sql(select_sql) %}
    select * from (
        {{ select_sql }}
    ) as __dbt_sbq
    where false
    limit 0
{% endmacro %}


{% macro snowflake__get_empty_subquery_sql(select_sql) %}
    select * from (
        {{ select_sql }}
    ) as __dbt_sbq
    where false
    limit 0
{% endmacro %}