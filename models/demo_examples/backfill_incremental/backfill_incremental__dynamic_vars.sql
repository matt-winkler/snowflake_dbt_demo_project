
{{
    config(
        materialized="incremental",
        incremental_strategy='merge',
        incremental_predicates=[
            "DBT_INTERNAL_DEST.order_date >= '1997-07-01'",
            "DBT_INTERNAL_DEST.order_date < CURRENT_DATE()"
        ],
        unique_key='order_key',
        on_schema_change='append_new_columns',
        partition_by=['order_date'],

    )
}}
  
{%- set get_default_start_date_sql -%}
  {%- if is_incremental() -%}
    SELECT MAX(order_date) FROM {{this}}
  {%- else -%}
    SELECT '1997-01-01'
  {%- endif -%}
{%- endset -%}

{%- set get_default_end_date_sql -%}
  SELECT CURRENT_DATE()
{%- endset -%}

{%- set start_date = var('start_date', dbt_utils.get_single_value(get_default_start_date_sql)) -%}
{%- set end_date = var('end_date', dbt_utils.get_single_value(get_default_end_date_sql)) -%} 

-- filter as early as possible in CTE chains
with upstream_data as (
    select * 
    from  {{ ref('fct_orders') }}
    where 1=1
    -- run as dbt build -s this_model --vars '{"start_date":"YYYY-MM-DD", "end_date":"YYYY-MM-DD"}'
    and order_date >= '{{ start_date }}'
    and order_date < '{{ end_date }}'
)

select *
from upstream_data