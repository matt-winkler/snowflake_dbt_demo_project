
{{
    config(
        materialized="incremental",
        incremental_strategy='delete+insert',
        unique_key='order_date',
        snowflake_warehouse='TRANSFORMING'
    )
}}

select * 
from {{ref('fct_orders')}}
where order_date > (select max(order_date) - 7 from {{this}})