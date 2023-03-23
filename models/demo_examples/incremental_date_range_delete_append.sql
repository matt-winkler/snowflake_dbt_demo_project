{{
    config(
        materialized='incremental',
        unique_key='order_date',
        incremental_strategy='append',
        pre_hook=delete_from_incremental_table_between(this, date_column='order_date')
    )
}}

select * 
from {{ref('fct_orders')}}
where order_date between '{{var('begin_date', '1992-01-01')}}' and '{{var('end_date', '1998-08-02')}}'
order by order_date -- don't specifically need the order by here but it will order the temp table.