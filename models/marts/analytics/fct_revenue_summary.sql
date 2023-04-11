{{
    config(
        materialized='view'
    )
}}

-- some comment to trigger another commit here.

select
        year(order_date) as year,
        sum(net_item_sales_amount) as total_revenue
from {{ ref('fct_orders') }}
group by 1