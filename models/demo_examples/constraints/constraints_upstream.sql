{{
    config(
        materialized='table'
    )
}}


with data as (
    select 1 as id
    union all
    select null as id
    union all
    select '000' as id
)

select * from data