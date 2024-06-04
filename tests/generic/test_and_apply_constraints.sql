{# 
  This has the effect of running the constraint SQL twice. 
  An alternative would be to run a similar check as a macro in a post hook instead. 
  Running as a macro would limit visibility into test status.
#}

{% test test_apply_constraints(model, column_name) %}

{# creates the test sql for validation #}
{% set validation_sql %}
  select
        {{ column_name }}
  from {{ model }}
  where {{ column_name }} is not null
  group by {{ column_name }}
  having count(*) > 1
{% endset %}

{% set result = dbt_utils.get_single_value(validation_sql) %}

{# apply the constraint if there is no result #}
{% if not result %}
  {% set constraint_sql %}
    alter table {{model}} add constraint unique_{{column_name}} unique({{column_name}}) rely;
  {% endset %}
  {% do run_query(constraint_sql) %}
{% endif %}

{# return the validation sql so the test command operates as normal #}
{{return(validation_sql)}}

{% endtest %}