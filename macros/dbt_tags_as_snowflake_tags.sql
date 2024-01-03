--macro.sql
{% macro dbt_tag_as_snowflake_object_tag(model) %}
  
  {% for tag in model.config.tags %}
    
    {% set tag_key = tag.split('=')[0] %}
    {% set tag_value = tag.split('=')[1] %}
    
    create tag if not exists {{tag_key}};
    alter table {{model.name}} set tag {{tag_key}}='{{tag_value}}';

  {% endfor %}

  {% for column in model.columns %}
    
    {% set column_tags = model.columns[column].tags %}

    {% for tag in column_tags %}
      
      {% set tag_key = tag.split('=')[0] %}
      {% set tag_value = tag.split('=')[1] %}
      
       create tag if not exists {{ tag_key }};

       ALTER TABLE {{ model.name }}
        MODIFY COLUMN {{ column }}
        SET TAG {{ tag_key }} = '{{ tag_value }}';

    {% endfor %}

  {% endfor %}
   
{% endmacro %}