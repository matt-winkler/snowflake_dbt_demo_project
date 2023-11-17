{{
    config(
        materialized = 'table',
        tags = ['finance', 'daily'],
        grants = {
            '+select': ['reporter']
        },
    )
}}

with order_item as (

    select * from {{ ref('order_items') }}
    
),

part_supplier as (

    select * from {{ ref('stg_tpch_part_suppliers')}}

),

dim_parts as (

    select * from {{ ref('dim_parts') }}

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
        order_item.base_price,
        order_item.discount_percentage,
        order_item.discounted_price,
        order_item.tax_rate,
        1 as order_item_count,
        order_item.quantity,
        order_item.gross_item_sales_amount,
        order_item.discounted_item_sales_amount,
        order_item.item_discount_amount,
        order_item.item_tax_amount,
        order_item.net_item_sales_amount,
        
        dim_parts.retail_price,

    from
        order_item
        inner join dim_parts 
            on order_item.part_key = dim_parts.part_key
        inner join part_supplier
            on order_item.part_key = part_supplier.part_key and
                order_item.supplier_key = part_supplier.supplier_key
)
select 
    *
from
    final
order by
    order_date