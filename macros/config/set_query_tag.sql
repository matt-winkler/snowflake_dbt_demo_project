{# 
Usage notes:
 - several dbt cloud environment variables are only populated in deployment environments
 - For example, there is no notion of a job Id for development runs of dbt
 - in this case and others, we populate a default value in the event the environment variable
 - isn't populated, e.g. env_var('DBT_CLOUD_JOB_ID', 'None')
 - read more on dbt cloud environment variables here: https://docs.getdbt.com/docs/build/environment-variables
#}

{% macro set_query_tag() -%}
    {% do return(dbt_snowflake_query_tags.set_query_tag(
        extra={
            'model_name': model.name,
            'database_name': target.database,
            'schema_name': target.schema,
            'warehouse_name': target.warehouse,
            'repo': var('repo'),
            'project_name': project_name,
            'dbt_version': dbt_version,
            'dbt_cloud_job_id': env_var('DBT_CLOUD_JOB_ID', 'None'),
            'dbt_run_id': env_var('DBT_CLOUD_RUN_ID', 'None'),
            'dbt_environment_type': env_var('DBT_CLOUD_ENVIRONMENT_TYPE'),
            'dbt_invocation_id': invocation_id,
            'dbt_cloud_run_reason_category': env_var('DBT_CLOUD_RUN_REASON_CATEGORY', 'None'),
            'dbt_cloud_run_reason': env_var('DBT_CLOUD_RUN_REASON', 'None'),
            'dbt_cloud_environment_id': env_var('DBT_CLOUD_ENVIRONMENT_ID', 'None'),
            'dbt_cloud_account_id': env_var('DBT_CLOUD_ACCOUNT_ID', 'None')
         }
    )) %}
{% endmacro %}
{% macro unset_query_tag(original_query_tag) -%}
    {% do return(dbt_snowflake_query_tags.unset_query_tag(original_query_tag)) %}
{% endmacro %}