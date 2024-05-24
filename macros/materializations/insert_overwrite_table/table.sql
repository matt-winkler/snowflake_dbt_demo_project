{% materialization table, adapter='snowflake', supported_languages=['sql', 'python']%}

  {% set original_query_tag = set_query_tag() %}

  {%- set identifier = model['alias'] -%}
  {%- set language = model['language'] -%}

  {% set grant_config = config.get('grants') %}
  {% set insert_overwrite_mode = config.get('insert_overwrite', False) %}
  {% set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') %}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database, type='table') -%}

  {{ run_hooks(pre_hooks) }}

  {#-- Drop the relation if it was a view to "convert" it in a table. This may lead to
    -- downtime, but it should be a relatively infrequent occurrence  #}
  {% if not insert_overwrite_mode %}

    {% if old_relation is not none and not old_relation.is_table %}
      {{ log("Dropping relation " ~ old_relation ~ " because it is of type " ~ old_relation.type) }}
      {{ drop_relation_if_exists(old_relation) }}
    {% endif %}

    {% call statement('main', language=language) -%}
      {{ create_table_as(False, target_relation, compiled_code, language) }}
    {%- endcall %}
  
  {% elif not old_relation %}
     
     {% call statement('main', language=language) -%}
      {{ create_table_as(False, target_relation, compiled_code, language) }}
    {%- endcall %}

  {% else %}
    
    {%- set tmp_relation = api.Relation.create(identifier=identifier+'__dbt_tmp',
                                                schema=schema,
                                                database=database, type='table') -%}
    {% call statement('main', language=language) -%}
      {{ create_table_as(False, tmp_relation, compiled_code, language) }}
    {%- endcall %}

    {% do adapter.expand_target_column_types(
           from_relation=tmp_relation,
           to_relation=target_relation) %}

    {% set dest_columns = process_schema_changes(on_schema_change, tmp_relation, target_relation) %}

    {% if not dest_columns %}
      {% set dest_columns = adapter.get_columns_in_relation(target_relation) %}
    {% endif %}
    
    {%- set insert_overwrite_sql = get_insert_overwrite_sql(target_relation, tmp_relation, dest_columns) -%}

    {% call statement('main', language=language) -%}
      {{ insert_overwrite_sql }}
    {%- endcall %}
  {% endif %}

  {{ run_hooks(post_hooks) }}

  {% set should_revoke = should_revoke(old_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {% do unset_query_tag(original_query_tag) %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}

{% macro py_write_table(compiled_code, target_relation, temporary=False, table_type=none) %}
{#- The following logic is only for backwards-compatiblity with deprecated `temporary` parameter -#}
{% if table_type is not none %}
    {#- Just use the table_type as-is -#}
{% elif temporary -%}
    {#- Case 1 when the deprecated `temporary` parameter is used without the replacement `table_type` parameter -#}
    {%- set table_type = "temporary" -%}
{% else %}
    {#- Case 2 when the deprecated `temporary` parameter is used without the replacement `table_type` parameter -#}
    {#- Snowflake treats "" as meaning "permanent" -#}
    {%- set table_type = "" -%}
{%- endif %}
{{ compiled_code }}
def materialize(session, df, target_relation):
    # make sure pandas exists
    import importlib.util
    package_name = 'pandas'
    if importlib.util.find_spec(package_name):
        import pandas
        if isinstance(df, pandas.core.frame.DataFrame):
          session.use_database(target_relation.database)
          session.use_schema(target_relation.schema)
          # session.write_pandas does not have overwrite function
          df = session.createDataFrame(df)
    {% set target_relation_name = resolve_model_name(target_relation) %}
    df.write.mode("overwrite").save_as_table('{{ target_relation_name }}', table_type='{{table_type}}')

def main(session):
    dbt = dbtObj(session.table)
    df = model(dbt, session)
    materialize(session, df, dbt.this)
    return "OK"
{% endmacro %}

{% macro py_script_comment()%}
# To run this in snowsight, you need to select entry point to be main
# And you may have to modify the return type to text to get the result back
# def main(session):
#     dbt = dbtObj(session.table)
#     df = model(dbt, session)
#     return df.collect()

# to run this in local notebook, you need to create a session following examples https://github.com/Snowflake-Labs/sfguide-getting-started-snowpark-python
# then you can do the following to run model
# dbt = dbtObj(session.table)
# df = model(dbt, session)
{%endmacro%}
