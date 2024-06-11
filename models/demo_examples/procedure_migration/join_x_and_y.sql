{{
    config(
        materialized='table',
    )
}}

select x.source_id
       ,x.order_id
       ,y.project_id
       ,y.campaign_id
       ,y.run_id
       ,y.tracking_segment_id
       ,y.response_ts   
       ,ROW_NUMBER() OVER(PARTITION BY x.source_id, x.order_id ORDER BY y.response_ts DESC,y.project_id DESC,y.campaign_id DESC,y.run_id DESC,y.tracking_segment_id DESC) row_num 
from {{ref('work_table_x')}} x
join {{ref('work_table_y')}} y
 on x.person_id = y.person_id
qualify row_num = 1