{% macro whats_meta() -%}
  {{config.get('meta') }}
{%- endmacro %}


{% macro get_foo() -%}
    {% if execute -%}
        execute is true and foo is {{ config.get('meta').get('foo') }}
    {%- else -%}
        execute is false
    {%- endif %}
{%- endmacro %}