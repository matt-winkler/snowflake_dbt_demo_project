{% macro set_unique_constraint(table, column_name, rely = true) %}

  {% set constraint_sql %}
      alter table {{table}} drop constraint unique_{{column_name}};
      alter table {{table}} add constraint unique_{{column_name}} unique({{column_name}}) {% if rely %}rely{% endif %};
  {% endset %}

  {{return(constraint_sql)}}

{% endmacro %}