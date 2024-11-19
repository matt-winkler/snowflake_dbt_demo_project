{{
    config(
        materialized='incremental',
        incremental_strategy='append',
    )
}}

select * from {{ref('node_temp')}}