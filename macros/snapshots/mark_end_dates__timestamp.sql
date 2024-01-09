{% macro mark_end_dates__timestamp(snapshot) %}
   
   {% if execute %}
   {% if check_snapshot_exists(snapshot) %}
     {% set unique_key = snapshot.config['unique_key'] %}
     {% set check_cols = snapshot.config['check_cols'] %}
  
     {% set create_tmp_table_sql %}

        create temp table {{snapshot.name}}__dbt_unique_key_tmp as (
          
          with multiple_null_keys as (
              select {{unique_key}} as unique_key, 
                   sum(case when dbt_valid_to is null then 1 else 0 end) as null_count
              from {{snapshot.database}}.{{snapshot.schema}}.{{snapshot.name}}
              group by 1
          ),
          
          internal_source as (
          select target.{{unique_key}} as unique_key,
                 target.dbt_scd_id,
                 target.updated_at,
                 row_number() over (partition by target.{{unique_key}} order by target.updated_at desc) as row_num,
                 case when dbt_valid_to is null 
                 then rtrim(cast(timestampadd(second, 1, target.dbt_valid_from) as varchar), '.000')  
                 else dbt_valid_to end 
                 
                 as dbt_valid_to
          from   {{snapshot.database}}.{{snapshot.schema}}.{{snapshot.name}} target
          join  multiple_null_keys
          on target.{{unique_key}} = multiple_null_keys.unique_key
          and multiple_null_keys.null_count > 1 
          where target.dbt_valid_to is null
          )
          
          select *
          from internal_source 
          where row_num != 1
        );
     {% endset %}
  
     {% set alter_snapshot_sql %}
        update {{snapshot.database}}.{{snapshot.schema}}.{{snapshot.name}} target
          set target.dbt_valid_to = tmp.dbt_valid_to
          from {{snapshot.name}}__dbt_unique_key_tmp tmp
          where target.dbt_scd_id = tmp.dbt_scd_id
          and   target.updated_at = tmp.updated_at
        ;
     {% endset %}
  
     {% set final_sql %}
        {{create_tmp_table_sql}} 
        {{alter_snapshot_sql}}
     {% endset %}

     {{return(final_sql)}}
  
  {% endif %}

  {% endif %}

{% endmacro %}