{{
    config(
        materialized='incremental',
        unique_key='id',
        incremental_strategy='delete+insert'
    )
}}

select * from {{ref('delete_insert_upstream')}}