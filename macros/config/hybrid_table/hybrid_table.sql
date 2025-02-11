{% materialization hybrid_table, adapter='snowflake' %}
  {%- set config = model['config'] -%}

  {% do log("Hybrid table materialization for model: " ~ model.name, info=True) %}
  {% do log("Config: " ~ config, info=True) %}
  {% do log("Target schema: " ~ target.schema, info=True) %}
  {% do log("Config schema: " ~ config.get('schema'), info=True) %}
  {% do log("Model schema: " ~ model.schema, info=True) %}

  {%- set target_schema = schema -%}
  
  {% do log("Final target schema: " ~ target_schema, info=True) %}

  {% set target_relation = api.Relation.create(
    database=target.database,
    schema=target_schema,
    identifier=model.alias
  ) %}

  {% do log("Target relation: " ~ target_relation, info=True) %}

  {%- set existing_relation = adapter.get_relation(
        database=target_relation.database,
        schema=target_relation.schema,
        identifier=target_relation.identifier) -%}

  {% do log("Existing relation: " ~ existing_relation, info=True) %}

  {%- set column_definitions = config.get('column_definitions', {}) -%}
  {%- set primary_key = config.get('primary_key', []) -%}
  {%- set primary_key = primary_key if primary_key is string else (primary_key | join(', ')) -%}
  {%- set indexes = config.get('indexes', []) -%}
  {%- set force_ctas = config.get('force_ctas', false) -%}

  -- Run pre-hooks
  {{ run_hooks(pre_hooks) }}

  {% if existing_relation is none or force_ctas %}
    -- Table doesn't exist or force_ctas is true, use CTAS
    {% call statement('main') -%}
      CREATE OR REPLACE HYBRID TABLE {{ target_relation }} (
        {% for column, definition in column_definitions.items() %}
          {{ column }} {{ definition }}{% if not loop.last %},{% endif %}
        {% endfor %}
        {% if primary_key %},
        PRIMARY KEY ({{ primary_key }})
        {% endif %}
        {% for index in indexes %}
        , INDEX {{ index.name }}({{ index.columns | join(', ') }})
        {% endfor %}
      ) AS (
        {{ sql }}
      )
    {%- endcall %}
  {% else %}
    -- Table exists, use MERGE
    {% call statement('main') -%}
      {{ sql }}
    {%- endcall %}
  {% endif %}

  -- Run post-hooks
  {{ run_hooks(post_hooks) }}

  -- Return the relations created in this materialization
  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
