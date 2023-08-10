{{ 
    config(
        materialized = 'view', 
        tags=['finance', 'daily']
    ) 
}}

{% set fact_orders_v0 = ref('fct_orders', v=0) %}

select
{{ dbt_utils.star(from=fact_orders_v0, except=["total_price"]) }}
from {{ fact_orders_v0 }}