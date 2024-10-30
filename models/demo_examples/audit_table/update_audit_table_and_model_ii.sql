{{
    config(
        materialized='incremental',
        unique_key = 'bar',
        incremental_strategy = 'merge',
        sql_header = "insert into audit_table select UUID_STRING() as id, CURRENT_TIMESTAMP() as _timestamp;"
    )
}}

select 'foo' as bar