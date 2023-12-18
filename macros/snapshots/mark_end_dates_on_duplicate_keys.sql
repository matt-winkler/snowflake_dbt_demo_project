{% macro mark_end_dates_on_duplicate_keys(snapshot) %}
   
   {% if execute %}
   {% if check_snapshot_exists(snapshot) %}
     {% set unique_key = snapshot.config['unique_key'] %}
     {% set check_cols = snapshot.config['check_cols'] %}
  
     {% set create_tmp_table_sql %}
        create temp table {{snapshot.name}}__dbt_unique_key_tmp as (
          with internal_source as (
          select {{unique_key}} as unique_key,
                 dbt_scd_id,
                 updated_at,
                 row_number() over (partition by {{unique_key}} order by updated_at) as row_num,
                 timestampadd(second, 1, dbt_valid_from) as dbt_valid_to
          from   {{snapshot.database}}.{{snapshot.schema}}.{{snapshot.name}} target
          join  (
            select {{unique_key}} as unique_key, 
                   sum(case when dbt_valid_to is null then 1 else 0 end) as null_count
            from {{snapshot.database}}.{{snapshot.schema}}.{{snapshot.name}}
            group by 1
          ) sub
          on target.{{unique_key}} = sub.unique_key
          and sub.null_count > 1 
          where target.dbt_valid_to is null
          )
          select unique_key,
                 dbt_scd_id,
                 {{dbt_utils.generate_surrogate_key(['unique_key', 'dbt_valid_to'])}} as dbt_scd_id_new,
                 updated_at,
                 dbt_valid_to
          from internal_source where row_num = 1
        );
     {% endset %}
  
     {% set alter_snapshot_sql %}
        update {{snapshot.database}}.{{snapshot.schema}}.{{snapshot.name}} target
          set target.dbt_valid_to = tmp.dbt_valid_to,
              target.dbt_scd_id = tmp.dbt_scd_id_new
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