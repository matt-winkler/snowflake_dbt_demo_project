{{
    config(
        materialized='incremental',
        unique_key='order_date',
        incremental_strategy='delete+insert',
        incremental_predicates=["order_date >= " ~ "'" ~ var('begin_date') ~ "'", "order_date <= " ~ "'" ~ var('end_date') ~ "'"],
    )
}}

select * 
from {{ref('fct_orders')}}
where order_date between '{{var('begin_date', '1992-01-01')}}' and '{{var('end_date', '1998-08-02')}}'
order by order_date -- don't specifically need the order by here but it will order the temp table.