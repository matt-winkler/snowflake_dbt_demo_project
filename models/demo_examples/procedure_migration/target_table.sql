{# 
  This process performs an incremental update on a table in Snowflake called target_table.
  - each of the subqueries could be independently run and tested in their own dbt models
  - after building the target table to the pre-production environment and passing tests, a downstream job rolls the updates forward using clones
#}

-- depends_on: {{ ref('product_master') }}

{{
    config(
        materialized='incremental',
        unique_key=['source_id', 'order_id'],
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        post_hook = update_audit_metadata()
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

{% if is_incremental() %}
source_3_update as (
    select p.prod_desc
           ,p.prod_class_desc
           ,CURRENT_TIMESTAMP(0) as cep_change_ts
           ,{{ln_in_batch_sk}} as cep_change_batch_sk
           ,tgt.source_id as source_id
           ,tgt.order_id as order_id
    from {{this}} tgt 
    join {{ref('product_master')}} p
      on tgt.base_prod_id = p.prod_id
),
{% endif %}

final as (
    {% if not is_incremental() %}
      select 1 as source_id, 100 as order_id, 2154121 as base_prod_id, CURRENT_TIMESTAMP as change_ts union all
      select 1 as source_id, 101 as order_id, 2910872 as base_prod_id, CURRENT_TIMESTAMP as change_ts union all 
      select 2 as source_id, 1000 as order_id, 6215211 as base_prod_id, CURRENT_TIMESTAMP as change_ts
    {% else %}
      select s1.source_id,
             s1.order_id,
             s1.cep_change_ts,
             coalesce(s2.click_project_id, s1.click_project_id) as click_project_id,
             coalesce(s2.click_campaign_id, s1.click_campaign_id) as click_campaign_id,
             coalesce(s2.click_run_id, s1.click_run_id) as click_run_id,
             coalesce(s2.click_tracking_segment_id, s1.click_tracking_segment_id) as click_tracking_segment_id,
             coalesce(s2.click_response_ts, s1.click_response_ts) as click_response_ts,
             s2.change_ts,
             s3.prod_desc,
             s3.prod_class_desc,
             -1111111 as base_prod_id
      from  source_1_update s1
      join source_2_update s2
        on  s1.source_id = s2.source_id
        and s1.order_id = s2.order_id
      join source_3_update s3 
        on  s1.source_id = s3.source_id
        and s1.order_id = s3.order_id
    {% endif %}
)

select * from final
