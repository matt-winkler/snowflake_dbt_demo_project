{% macro monthly_gross_revenue_proc(start_date, end_date, dry_run=True) %}

{# dbt run-operation my_awesome_stored_proc --args '{"start_date":"1998-01-01", "end_date":"1998-01-31", "dry_run": False}' #}

{% set sql %}
    
    create or replace procedure monthly_gross_revenue_proc(start_date date, end_date date)
    returns varchar not null
    language sql
    as
    $$
    begin
    create or replace view ANALYTICS.dbt_mwinkler.stg_tpch_line_items
  
   as (
    with source as (

    select * from raw.tpch_sf001.lineitem

),

renamed as (

    select
    
        md5(cast(coalesce(cast(l_orderkey as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(l_linenumber as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT))
                as order_item_key,
        l_orderkey as order_key,
        l_partkey as part_key,
        l_suppkey as supplier_key,
        l_linenumber as line_number,
        l_quantity as quantity,
        l_extendedprice as extended_price,
        l_discount as discount_percentage,
        l_tax as tax_rate,

        case l_returnflag
            when 'R' then 'returned'
            when 'N' then 'normal'
            when 'A' then 'awaiting return'
            else null
        end as return_flag, 

        case l_linestatus 
            when 'P' then 'returned'
            when 'F' then 'billed'
            when 'O' then 'shipped'
            else null
        end as status_code,
        
        l_shipdate as ship_date,
        l_commitdate as commit_date,
        l_receiptdate as receipt_date,
        l_shipinstruct as ship_instructions,
        l_shipmode as ship_mode,
        l_comment as comment

    from source

)

select * from renamed
  );

create or replace view ANALYTICS.dbt_mwinkler.stg_tpch_orders
  
   as (
    with source as (

    select * from raw.tpch_sf001.orders

),

rename as (

    select
    
        o_orderkey as order_key,
        o_custkey as customer_key,
        o_orderstatus as status_code,
        o_totalprice as total_price,
        o_orderdate as order_date,
        o_orderpriority as priority_code,
        o_clerk as clerk_name,
        o_shippriority as ship_priority,
        o_comment as comment

    from source

)

select * from rename
  );


create or replace temp table ANALYTICS.dbt_mwinkler.stg_tpch_part_suppliers
  
   as (
    with source as (

    select * from raw.tpch_sf001.partsupp

),

renamed as (

    select
    
        md5(cast(coalesce(cast(ps_partkey as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(ps_suppkey as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) 
                as part_supplier_key,
        ps_partkey as part_key,
        ps_suppkey as supplier_key,
        ps_availqty as available_quantity,
        ps_supplycost as cost,
        ps_comment as comment

    from source

)

select * from renamed
  );

create or replace temp table ANALYTICS.dbt_mwinkler.stg_tpch_parts
  
   as (
    with source as (

    select * from raw.tpch_sf001.part

),

renamed as (

    select
    
        p_partkey as part_key,
        p_name as name,
        p_mfgr as manufacturer,
        p_brand as brand,
        p_type as type,
        p_size as size,
        p_container as container,
        p_retailprice as retail_price,
        p_comment as comment

    from source

)

select * from renamed
  );

create or replace   view ANALYTICS.dbt_mwinkler.stg_tpch_suppliers
  
   as (
    with source as (

    select * from raw.tpch_sf001.supplier

),

renamed as (

    select
    
        s_suppkey as supplier_key,
        s_name as supplier_name,
        s_address as supplier_address,
        s_nationkey as nation_key,
        s_phone as phone_number,
        s_acctbal as account_balance,
        s_comment as comment

    from source

)

select * from renamed
  );

create or replace transient table ANALYTICS.dbt_mwinkler.order_items
         as
        (

with orders as (
    
    select * from ANALYTICS.dbt_mwinkler.stg_tpch_orders

),

line_item as (

    select * from ANALYTICS.dbt_mwinkler.stg_tpch_line_items

)
select 

    line_item.order_item_key,
    orders.order_key,
    orders.customer_key,
    line_item.part_key,
    line_item.supplier_key,
    orders.order_date,
    orders.status_code as order_status_code,
    
    
    line_item.return_flag,
    
    line_item.line_number,
    line_item.status_code as order_item_status_code,
    line_item.ship_date,
    line_item.commit_date,
    line_item.receipt_date,
    line_item.ship_mode,
    line_item.extended_price,
    line_item.quantity,
    
    -- extended_price is actually the line item total,
    -- so we back out the extended price per item
    (line_item.extended_price/nullif(line_item.quantity, 0))::decimal(16,3) as base_price,
    line_item.discount_percentage,
    (base_price * (1 - line_item.discount_percentage))::decimal(16,3) as discounted_price,

    line_item.extended_price as gross_item_sales_amount,
    (line_item.extended_price * (1 - line_item.discount_percentage))::decimal(16,3) as discounted_item_sales_amount,
    -- We model discounts as negative amounts
    (-1 * line_item.extended_price * line_item.discount_percentage)::decimal(16,3) as item_discount_amount,
    line_item.tax_rate,
    ((gross_item_sales_amount + item_discount_amount) * line_item.tax_rate)::decimal(16,3) as item_tax_amount,
    (
        gross_item_sales_amount + 
        item_discount_amount + 
        item_tax_amount
    )::decimal(16,3) as net_item_sales_amount

from
    orders
inner join line_item
        on orders.order_key = line_item.order_key
order by
    orders.order_date
        );

create or replace   view ANALYTICS.dbt_mwinkler.part_suppliers
  
   as (
    with part as (
    
    select * from ANALYTICS.dbt_mwinkler.stg_tpch_parts

),

supplier as (

    select * from ANALYTICS.dbt_mwinkler.stg_tpch_suppliers

),

part_supplier as (

    select * from ANALYTICS.dbt_mwinkler.stg_tpch_part_suppliers

),

final as (
    select 

    part_supplier.part_supplier_key,
    part.part_key,
    part.name as part_name,
    part.manufacturer,
    part.brand,
    part.type as part_type,
    part.size as part_size,
    part.container,
    part.retail_price,

    supplier.supplier_key,
    supplier.supplier_name,
    supplier.supplier_address,
    supplier.phone_number,
    supplier.account_balance,
    supplier.nation_key,

    part_supplier.available_quantity,
    part_supplier.cost
from
    part 
inner join 
    part_supplier
        on part.part_key = part_supplier.part_key
inner join
    supplier
        on part_supplier.supplier_key = supplier.supplier_key
order by
    part.part_key
)

select * from final
  );

create or replace transient table ANALYTICS.dbt_mwinkler.fct_order_items
         as
        (

with order_item as (

    select * from ANALYTICS.dbt_mwinkler.order_items
    
),
part_supplier as (
    
    select * from ANALYTICS.dbt_mwinkler.part_suppliers

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
        
        part_supplier.retail_price,
        'bbb' as test,
        order_item.base_price,
        order_item.discount_percentage,
        order_item.discounted_price,
        order_item.tax_rate,
        part_supplier.nation_key,
        1 as order_item_count,
        order_item.quantity,
        order_item.gross_item_sales_amount,
        order_item.discounted_item_sales_amount,
        order_item.item_discount_amount,
        order_item.item_tax_amount,
        order_item.net_item_sales_amount

    from
        order_item
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
        );

    grant select on table analytics.dbt_mwinkler.fct_order_items to role reporter;

    end;
    $$;

    call monthly_gross_revenue_proc('{{start_date}}', '{{end_date}}')
{% endset %}

{% if dry_run %}
  {{log(sql, info=true)}}
{% else %}
  {% do run_query(sql) %}
{% endif %}

{% endmacro %}