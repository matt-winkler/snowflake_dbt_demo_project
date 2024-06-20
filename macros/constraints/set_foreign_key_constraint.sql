{% macro set_foreign_key_constraint(this_table, this_column_name, fk_table, fk_column, rely = true) %}

  {% set constraint_sql %}
      alter table {{this_table}} drop constraint foreign_key_{{this_column_name}};
      alter table {{this_table}} add constraint foreign_key_{{this_column_name}} foreign key ({{this_column_name}}) references {{fk_table}} ({{fk_column}}) {% if rely %}rely{% endif %};
  {% endset %}

  {{return(constraint_sql)}}

{% endmacro %}