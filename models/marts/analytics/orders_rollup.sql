{{
    config(
        materialized='view'
    )
}}

select
        year(order_date) as year,
        month(order_date) as month,
        day(order_date) as day,
        sum(net_item_sales_amount) as total_revenue
from {{ ref('fct_orders') }}
group by 1,2,3