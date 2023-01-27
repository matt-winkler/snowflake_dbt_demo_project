{{
    config(
        materialized='view'
    )
}}

select
        year(order_date) as year,
        sum(net_item_sales_amount) as total_revenue
from {{ ref('fct_orders') }}
where year <> year(current_timestamp())
group by 1