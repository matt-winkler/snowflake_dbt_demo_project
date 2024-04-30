{{
    config(
        materialized = 'dynamic_table',
        snowflake_warehouse = 'TRANSFORMING',
        target_lag = '5 minutes',
        on_configuration_change = 'apply',
        enabled=False
    )
}}

select * from {{ref('fct_order_items')}}