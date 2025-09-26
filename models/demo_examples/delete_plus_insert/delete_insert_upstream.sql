{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite'
    )
}}

select 1 as id