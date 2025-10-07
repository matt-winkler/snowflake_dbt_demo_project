{{
    config(
        materialized='table',
        meta = {"pre_hook": "select 'foo' as bar"}

    )
}}

select 1 as id