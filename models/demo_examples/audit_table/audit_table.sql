{{
    config(
        materialized='table'
    )
}}

select UUID_STRING() as id, CURRENT_TIMESTAMP() as _timestamp