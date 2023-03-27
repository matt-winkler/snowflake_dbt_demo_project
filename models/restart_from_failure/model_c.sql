{{
    config(
        materialized='table'
    )
}}

select 1 as id
union
-- failure due to data issue
select 'aaa' as id