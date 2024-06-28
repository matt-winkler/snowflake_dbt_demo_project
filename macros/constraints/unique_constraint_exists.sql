{% macro unique_constraint_exists(table_relation, column_names) %}
  {{return(adapter.dispatch('unique_constraint_exists')(table_relation, column_names))}}
{% endmacro %}

{%- macro snowflake__unique_constraint_exists(table_relation, column_names) -%}

{%- set lookup_query -%}
SHOW UNIQUE KEYS IN TABLE {{ table_relation }}
{%- endset -%}
{%- set constraint_list = run_query(lookup_query) -%}
{%- if constraint_list.columns["column_name"].values() | count > 0 -%}
        {%- for constraint in constraint_list.group_by("constraint_name") -%}

            {%- if column_list_matches(constraint.columns["column_name"].values(), column_names ) -%}
                {%- do log("Found UK key: " ~ table_relation ~ " " ~ column_names, info=false) -%}
                {{ return(true) }}
            {%- endif -%}
        {% endfor %}
    {%- endif -%}

{%- set lookup_query -%}
SHOW PRIMARY KEYS IN TABLE {{ table_relation }}
{%- endset -%}
{%- set constraint_list = run_query(lookup_query) -%}
{%- if constraint_list.columns["column_name"].values() | count > 0 -%}
        {%- for constraint in constraint_list.group_by("constraint_name") -%}
            {%- if column_list_matches(constraint.columns["column_name"].values(), column_names ) -%}
                {%- do log("Found PK key: " ~ table_relation ~ " " ~ column_names, info=false) -%}
                {{ return(true) }}
            {%- endif -%}
        {% endfor %}
    {%- endif -%}

{#- If we get this far then the table does not have either constraint -#}
{%- do log("No PK/UK key: " ~ table_relation ~ " " ~ column_names, info=false) -%}
{{ return(false) }}
{%- endmacro -%}