
{{
    config(
        materialized="incremental",
        incremental_strategy='merge',
        unique_key='order_key',
        merge_update_columns=['order_key', 'ship_priority']

    )
}}

select *
from  {{ref('fct_orders')}}
{% if is_incremental() %}
where order_key not in (select order_key from {{this}} group by 1)
{% else %}
where order_date < '1998-07-01'
{% endif %}
