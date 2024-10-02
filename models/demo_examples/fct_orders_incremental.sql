
{{
    config(
        materialized="incremental",
        incremental_strategy='microbatch',
        event_time='order_date',
        batch_size='year',
        begin='1992-01-01',
        snowflake_warehouse='TRANSFORMING'
    )
}}

select * 
from {{ref('fct_orders')}}