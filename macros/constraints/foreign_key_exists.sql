{% macro foreign_key_exists(table_relation, column_names) %}
  {{return(adapter.dispatch('foreign_key_exists')(table_relation, column_names))}}
{% endmacro %}

{%- macro snowflake__foreign_key_exists(table_relation, column_names) -%}

{%- set lookup_query -%}
SHOW IMPORTED KEYS IN TABLE {{ table_relation }}
{%- endset -%}
{%- set constraint_list = run_query(lookup_query) -%}
{%- if constraint_list.columns["fk_column_name"].values() | count > 0 -%}
        {%- for constraint in constraint_list.group_by("fk_name") -%}
            {%- if dbt_constraints.column_list_matches(constraint.columns["fk_column_name"].values(), column_names ) -%}
                {%- do log("Found FK key: " ~ table_relation ~ " " ~ column_names, info=false) -%}
                {{ return(true) }}
            {%- endif -%}
        {% endfor %}
    {%- endif -%}

{#- If we get this far then the table does not have this constraint -#}
{%- do log("No FK key: " ~ table_relation ~ " " ~ column_names, info=false) -%}
{{ return(false) }}
{%- endmacro -%}