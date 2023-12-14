{% macro get_source_configs(graph) %}
  
  {% if execute %}
    {% set sources = graph.get('sources').values() | list %}

    {% set sql %}

    {% for source in sources %}
       select '{{source.identifier}}' as source_identifier,
              '{{source.database}}' as source_database,
              '{{source.schema}}' as source_schema,
              '{{source.package_name}}' as source_package_name 
      {% if not loop.last -%}
        union all
      {%- endif %}

    {% endfor %}
    {% endset %}

    {{ return(sql) }}

  {% endif %}

{% endmacro %}