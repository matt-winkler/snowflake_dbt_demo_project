
{{
  config(
    materialized = 'table'
    )
}}

select 1 as id, 'foo' as bar union all
select 1 as id, 'foo' as bar
--select 1 as id, null as bar