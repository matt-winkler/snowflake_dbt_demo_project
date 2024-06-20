
{{
  config(
    materialized = 'table'
    )
}}

select 1 as id, 'foo' as col1 union all
select 2 as id, 'bar' as col1 union all 
select 3 as id, 'baz' as col1