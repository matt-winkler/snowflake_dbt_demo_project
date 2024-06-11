{% macro create_python_udf() %}
  {% set sql -%}
    use database {{target.database}};
   
    create or replace function {{target.schema}}.addone(i int)
    returns int
    language python
    runtime_version = '3.8'
    handler = 'addone_py'
    as
    $$
def addone_py(i):
    return i+1
    $$;
  {%- endset %}

  {% do log(sql, info=true) %}
  {% do run_query(sql) %}
{% endmacro %}