select 
    date_trunc(MONTH, order_date) as order_month
    , sum(gross_item_sales_amount) as gross_revenue
from {{ ref('fct_order_items') }}
group by 1