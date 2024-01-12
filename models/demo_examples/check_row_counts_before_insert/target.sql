{{
    config(
        materialized='table'
    )
}}

select * from {{ref('upstream')}}