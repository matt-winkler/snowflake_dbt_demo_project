{{
    config(
        materialized = 'table',
        tags = ['finance'],
        grants = {
            '+select': ['reporter']
        },
    )
}}

with order_item as (

    select * from {{ ref('order_items') }}
    
),
part_supplier as (
    
    select * from {{ ref('part_suppliers') }}

),

final as (

    select 
        order_item.order_item_key,
        order_item.order_key,
        order_item.order_date,
        order_item.customer_key,
        order_item.part_key,
        order_item.supplier_key,
        order_item.order_item_status_code,
        order_item.return_flag,
        order_item.line_number,
        order_item.ship_date,
        order_item.commit_date,
        order_item.receipt_date,
        order_item.ship_mode,
        part_supplier.cost as supplier_cost,
        {# part_supplier.retail_price, #} -- commented out this column
        order_item.base_price,
        order_item.discount_percentage,
        order_item.discounted_price,
        order_item.tax_rate,
        part_supplier.nation_key,
        1 as order_item_count,
        case when order_date = '1992-01-01' then 100 else order_item.quantity end as quantity, -- added this case statement
        order_item.gross_item_sales_amount as gross_item_revenue, -- renamed this column
        order_item.discounted_item_sales_amount,
        order_item.item_discount_amount,
        order_item.item_tax_amount,
        order_item.net_item_sales_amount

    from
        order_item
        inner join part_supplier
            on order_item.part_key = part_supplier.part_key and
                order_item.supplier_key = part_supplier.supplier_key
    where order_date <= '1992-01-17' -- added a date filter
)
select 
    *
from
    final
order by
    order_date