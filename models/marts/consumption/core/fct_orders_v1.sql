{{ 
    config(
        materialized = 'view', 
    ) 
}}

select
{{ dbt_utils.star(from = ref('fct_orders', v=0), except=["total_price"]) }}
from {{ ref('fct_orders', v=0) }}