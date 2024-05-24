
{{
  config(
    materialized = 'table',
    insert_overwrite = True,
    on_schema_change = 'ignore'
    )
}}

select 1 as id, 'foo' as bar union all
select 1 as id, 'foo' as bar union all
select 1 as id, null as bar