{% macro validate_meta_tagging(model_to_validate=none) %}

   {% if not execute %}
     {{ return('') }}
   {% endif %}

   {% set model_config = model_to_validate.config %}
   {% set name = model_to_validate.name %}
   
   {% if model_config.get("materialized", "") not in ("", "ephemeral") %}
     {{ log(model_config, info=True) }}
     {% if 'foo' not in model_config['meta'] %}
       {{ exceptions.raise_compiler_error("model " ~ name ~ " is missing required tag: foo") }}
     {% endif %}
   {% endif %}

{% endmacro %}