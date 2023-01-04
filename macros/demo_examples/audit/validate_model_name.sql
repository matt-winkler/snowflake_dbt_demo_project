{% macro validate_model_name(model, ruleset=None) %}

  {% if ruleset == None %}
      {{ return() }}
  
  {% elif ruleset == 'stage' %}  
    {% if not model.identifier.startswith('stg_') %}
      {{ exceptions.raise_compiler_error("Invalid model name validation. Staging models must start with 'stg_'. Got: " ~ model.identifier) }}
    {% endif %}
  
  {% elif ruleset == 'demos' %}
    {% if not model.identifier.startswith('demos_') %}
      {{ exceptions.raise_compiler_error("Invalid model name validation. Demo models must start with 'demos_'. Got: " ~ model.identifier) }}
    {% endif %}
  
  {% else %}  
    {{ exceptions.raise_compiler_error("Invalid model name validation ruleset. Got: " ~ ruleset) }}
  {% endif %}
{% endmacro %}