{# This overrides projects/xxxx/dbt_packages/elementary/macros/edr/tests/on_run_end/handle_tests_results.sql's insert_data_monitoring_metrics() macro #}
{%- macro get_single_value(query, default) -%}
    {{ return(adapter.dispatch('get_single_value')(query, default)) }}
{%- endmacro -%}


{% macro default__get_single_value(query, default=none) %}

{# This macro returns the (0, 0) record in a query, i.e. the first row of the first column #}
    {% do log('overloading get_single_value macro', info=true) %}

    {%- call statement('get_query_result', fetch_result=True, auto_begin=false) -%}

        {{ query }}

    {%- endcall -%}

    {%- if execute -%}

        {% set r = load_result('get_query_result').table.columns[0].values() %}
        {% if r | length == 0 %}
            {% do print('Query `' ~ query ~ '` returned no rows. Using the default value: ' ~ default) %}
            {% set sql_result = default %}
        {% else %}
            {% set sql_result = r[0] %}
        {% endif %}
        
    {%- else -%}
    
        {% set sql_result = default %}
    
    {%- endif -%}

    {% do return(sql_result) %}

{% endmacro %}