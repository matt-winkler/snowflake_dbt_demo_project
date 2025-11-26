{% macro snowflake__get_column_data_type_for_alter(relation, column) %}
  {#
    Helper macro to get the correct data type for ALTER TABLE operations.
    For Iceberg tables, we need to handle VARCHAR constraints differently because
    Snowflake Iceberg tables only support max length (134,217,728) or STRING directly.

    This fixes the bug where dbt generates VARCHAR(16777216) for new columns which
    is not supported by Snowflake Iceberg tables.
  #}
  {% if relation.is_iceberg_format and column.is_string() %}
    {% set data_type = column.data_type.upper() %}
    {% if data_type == 'CHARACTER VARYING(16777216)' %}
       {% set msg = 'data type error on column ' ~ column.name ~ ': Snowflake no longer uses CHARACTER VARYING(16777216) 
                    as the default max string length. Pass CHARACTER VARYING(134217728) instead'
        %}
        {% do exceptions.raise_compiler_error(msg) %}
    {% endif %}
    {% if data_type.startswith('CHARACTER VARYING') or data_type.startswith('VARCHAR') %}
      {#
        For Iceberg tables, convert any VARCHAR specification to STRING.
        This handles cases where:
        - dbt auto-generates VARCHAR(16777216) for columns without explicit size
        - users specify VARCHAR with any size (even the max 134217728)
        Using STRING is more compatible and avoids size-related errors.
        16777216
        134217728
      #}
      STRING
    {% else %}
      {# Keep other string types like TEXT as-is #}
      {{ column.data_type }}
    {% endif %}
  {% else %}
    {{ column.data_type }}
  {% endif %}
{% endmacro %}