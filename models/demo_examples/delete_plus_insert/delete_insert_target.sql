{{
    config(
        materialized='incremental',
        unique_key='id',
        incremental_strategy='delete+insert',
        meta = {"pre_hook": "select 'foo' as bar"}
    )
}}

select * from {{ref('delete_insert_upstream')}}