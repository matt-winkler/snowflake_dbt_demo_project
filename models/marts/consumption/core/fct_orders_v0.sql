{{
    config(
        materialized = 'table',
        tags=['finance'],
        grants = {
            '+select': ['reporter']
        },
    )
}}

with orders as (

    select * from {{ ref('stg_tpch_orders') }}

),

order_items as (

    select * from {{ ref('order_items') }}

),

customers as (
    select * from {{ ref('dim_customers' )}}
)

order_item_summary as (

    select
        order_key,
        sum(gross_item_sales_amount) as gross_item_sales_amount,
        sum(item_discount_amount) as item_discount_amount,
        sum(item_tax_amount) as item_tax_amount,
        sum(net_item_sales_amount) as net_item_sales_amount,
        count_if(return_flag = 'returned') as return_count
    from order_item
    group by
        1
),

final as (

    select

        orders.order_key,
        orders.order_date,
        orders.customer_key,
        orders.status_code,
        orders.priority_code,
        orders.clerk_name,
        orders.ship_priority,
        customers.region,
        1 as order_count,
        orders.total_price,
        order_items.return_count,
        order_items.gross_item_sales_amount,
        order_items.item_discount_amount,
        order_items.item_tax_amount,
        order_items.net_item_sales_amount
    from
        orders
    inner join order_items
        on orders.order_key = order_items.order_key
    inner join customers
        on orders.customer_key = customers.customer_key
)

select *
from
    final

order by
    order_date
