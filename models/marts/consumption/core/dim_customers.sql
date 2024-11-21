{{
    config(
        materialized = 'table',
        transient=false,
        tags=['customers', 'weekly']
    )
}}

with customer as (

    select * from {{ ref('stg_tpch_customers') }}

),
nation as (

    select * from {{ ref('stg_tpch_nations') }}
),
region as (

    select * from {{ ref('stg_tpch_regions') }}

),


first_order_date as (

    select customer_key, min(order_date) as first_order_date
    from   {{ref('fct_order_items')}}
    group by 1
),


final as (
    select 
        customer.customer_key,
        customer.name,
        customer.address,
        {# nation.nation_key as nation_key, #}
        nation.name as nation,
        {# region.region_key as region_key, #}
        region.name as region,
        customer.phone_number,
        customer.account_balance,
        customer.market_segment
        ,first_order_date.first_order_date
    from
        customer
        inner join nation
            on customer.nation_key = nation.nation_key
        inner join region
            on nation.region_key = region.region_key
        inner join first_order_date
           on customer.customer_key = first_order_date.customer_key
)
select 
    *
from
    final
order by
    customer_key