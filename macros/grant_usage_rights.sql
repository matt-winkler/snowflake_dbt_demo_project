{% macro grant_usage_rights(database_schemas, role) %}
  {% if target.name == 'prod' %}
    {% for (database, schema) in database_schemas %}
      grant usage on database {{ database }} to role {{ role }};
      grant usage on schema {{ schema }} to role {{ role }};
      grant select on all tables in schema {{ schema }} to role {{ role }};
      grant select on all views in schema {{ schema }} to role {{ role }};
      grant select on future tables in schema {{ schema }} to role {{ role }};
      grant select on future views in schema {{ schema }} to role {{ role }};
    {% endfor %}
  {% endif %}
{% endmacro %}