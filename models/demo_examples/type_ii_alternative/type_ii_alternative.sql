{{
    config(
        materialized='incremental',
        unique_key=['lead_id','status','source'],
        incremental_strategy='merge'
    )
}}

with upstream_data as (

--select	'2024-09-16'	as dt,	1	as lead_id,	'2024-09-16'	as created_dt,	'valid'	as status,	'chat'	as source,	'demo'	as offer,	'dell'	as company,	'2024-09-16'	as update_dt	union all
--select	'2024-09-16'	as dt,	2	as lead_id,	'2024-09-16'	as created_dt,	'valid'	as status,	'chat'	as source,	'demo'	as offer,	'dell'	as company,	'2024-09-16'	as update_dt	union all
--select	'2024-09-16'	as dt,	3	as lead_id,	'2024-09-16'	as created_dt,	'valid'	as status,	'chat'	as source,	'demo'	as offer,	'dell'	as company,	'2024-09-16'	as update_dt

--select	'2024-09-17'	as dt,	1	as lead_id,	'2024-09-16'	as created_dt,	'invalid'	as status,	'chat'	as source,	'demo'	as offer,	'dell'	as company,	'2024-09-17'	as update_dt

select	'2024-09-18'	as dt,	4	as lead_id,	'2024-09-16'	as created_dt,	'valid'	as status,	'chat'	as source,	'demo'	as offer,	'dell'	as company,	'2024-09-18'	as update_dt union all

select	'2024-09-19'	as dt,	2	as lead_id,	'2024-09-16'	as created_dt,	'valid'	as status,	'chat'	as source,	'trial'	as offer,	'dell'	as company,	'2024-09-19'	as update_dt

),

inserts as (
    select 
          u.lead_id,
          u.dt as created_dt,
          u.status,
          u.source,
          u.offer,
          u.company,
          TO_DATE(u.update_dt) as rec_start_dt,
          TO_DATE('9999-12-31') as rec_end_dt,
          'Y' as curr_flag
    from upstream_data u
),

{% if is_incremental() %}
updates as (

    select 
           t.lead_id,
           t.created_dt,
           t.status,
           t.source,
           t.offer,
           t.company,
           t.rec_start_dt,
           i.rec_start_dt - 1 as rec_end_dt,
          'N' as curr_flag
    from   {{this}} t
    join   inserts i 
      on t.lead_id = i.lead_id
      and (
        (t.status != i.status) OR
        (t.source != i.source)
      )
    where  t.curr_flag = 'Y'

),
unioned as (
    select * from inserts union all
    select * from updates
)

{% else %}
unioned as (
    select * from inserts
)
{% endif %}

select * 
from unioned