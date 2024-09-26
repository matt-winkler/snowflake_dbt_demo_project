{{
    config(
        materialized='incremental',
        incremental_strategy='append',
    )
}}

select * from {{ref('es_nodes_temp')}}