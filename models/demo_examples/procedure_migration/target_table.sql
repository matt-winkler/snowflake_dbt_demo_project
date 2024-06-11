{# 
  This process performs an incremental update on a table in Snowflake called target_table.
  - each of the subqueries could be independently run and tested in their own dbt models
  - after building the target table to the pre-production environment and passing tests, a downstream job rolls the updates forward using clones

#}

{{
    config(
        materialized='incremental',
        unique_key=['source_id', 'order_id'],
        incremental_strategy='merge',
    )
}}

{# this could be a runtime parameter, the result of a query, etc. #}
{% set ln_in_batch_sk = 4128903721 %} 

with source_1_update as (
    select yy.source_id, 
           yy.order_id,
           CURRENT_TIMESTAMP(0) as cep_change_ts,
           'aaa' as click_project_id,
           'default' as click_campaign_id,
           'default' as click_run_id,
           'default' as click_node_id,
           'default' as click_branch_id,
            NULL as click_response_ts,
            NULL as click_launch_ts,
            NULL as click_tracking_segment_id,
            NULL as click_target_country,	
            NULL as click_target_region,	
            NULL click_delivery_locale_cd, 
            {{ln_in_batch_sk}} as cep_change_batch_sk
    from {{ref('work_table_yy')}} yy 
    {% if is_incremental() %}
      join {{this}} tgt -- {{this}} is a dbt built in variable referring to the target table itself
        on yy.source_id = tgt.source_id
        and yy.order_id = tgt.order_id
    {% endif %}
),

source_2_update  as (
    select zz.source_id
           ,zz.order_id
           ,zz.project_id as click_project_id
           ,zz.campaign_id as click_campaign_id
           ,zz.run_id as click_run_id
           ,zz.tracking_segment_id as click_tracking_segment_id
           ,zz.response_ts as click_response_ts
           ,CURRENT_TIMESTAMP(0) as change_ts
           ,{{ln_in_batch_sk}} as cep_change_batch_sk
    from {{ref('join_x_and_y')}} zz
    {% if is_incremental() %}
      join {{this}} tgt
       on  zz.source_id = tgt.source_id
       and zz.order_id = tgt.order_id
    {% endif %}
),

source_3_update as (
    select p.prod_desc
           ,p.prod_class_desc
           ,p.prod_sub_family_id
           ,p.prod_id
           ,CURRENT_TIMESTAMP(0) as cep_change_ts,
           ,{{ln_in_batch_sk}} as cep_change_batch_sk
    from {{ref('product_master')}} p
    {% if is_incremental() %}
      join {{this}} tgt
        on p.prod_id = tgt.prod_id
    {% endif %}
)

final as (
    select 
)

