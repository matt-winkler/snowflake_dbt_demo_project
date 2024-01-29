"inline_query": -- depends_on: {{ ref('fct_orders') }}

{{
    config(
        materialized='incremental',
        unique_key='order_id'
    )
}}

{% set min_date = get_min_of_mins([('fct_orders', 'order_date')]) %}
{# get_min_of_maxs #}

with orders_data as (
   select * 
   from {{ref('fct_orders')}}
   where order_date > '{{min_date}}'
)
