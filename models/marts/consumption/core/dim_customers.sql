{{
    config(
        materialized = 'table',
        transient=false,
        tags=['customers', 'weekly'],
    )
}}


with customer as (

    select * from {{ ref('stg_tpch_customers') }} -- dbt is smart enough to read from production, if the upstream 
    -- model does not exist. This means time and cost savings by not rebuilding data in development environments.
    -- this is an opt in feature: permissions to read from prod are still fully controlled on Snowflake

),
nation as (

    select * from {{ ref('stg_tpch_nations') }}
),
region as (

    select * from {{ ref('stg_tpch_regions') }}

),


most_recent_order_date as (
    select customer_key, min(order_date) as most_recent_order_date
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
        customer.market_segment,
        --'2024-01-01'::date as most_recent_order_date
        most_recent_order_date.most_recent_order_date
    from
        customer
        inner join nation
            on customer.nation_key = nation.nation_key
        inner join region
            on nation.region_key = region.region_key
        inner join most_recent_order_date
            on customer.customer_key = most_recent_order_date.customer_key
)
select 
    *
from
    final
order by
    customer_key