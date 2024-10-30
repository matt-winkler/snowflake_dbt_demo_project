{{
    config(
        materialized='incremental',
        unique_key = 'bar',
        incremental_strategy = 'merge',
        pre_hook = [
            before_begin("select 1 as id"),
            in_transaction("insert into audit_table select UUID_STRING() as id, CURRENT_TIMESTAMP() as _timestamp")
        ]
    )
}}

select 'foo' as bar