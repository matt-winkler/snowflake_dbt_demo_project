{% macro truncate_all_tables_in_schema(schema_name, dry_run=True) %}
  
  {% set sql %}
    show tables in schema {{schema_name}};
  {% endset %}
  
  {% if dry_run %}
     {{log(sql, info=true)}}
  {% endif %}

  {% if not dry_run %}
  
    {% set tables = run_query(sql) %}

    {% for table in tables %}
      {% set sql %}
        truncate table {{schema_name}}.{{table}};
      {% endset %}

      {{ run_query(sql) }}

    {% endfor %}
  
  {% endif %}

{% endmacro %}