
/* Create a pivot table with dynamic columns based on the ship modes that are in the system */

{% set ship_modes = get_distinct_column_values(ref('fct_order_items'), 'ship_mode') %}

select
    date_part('year', order_date) as order_year,

    {# Loop over ship_modes array from above, and sum based on whether the record matches the ship mode #}
    {%- for ship_mode in ship_modes -%}
        sum(case when ship_mode = '{{ship_mode}}' then gross_item_sales_amount end) as "{{ship_mode|replace(' ', '_')}}_amount"
        {%- if not loop.last -%},{% endif %}
    {% endfor %}

from {{ ref('fct_order_items') }}
group by 1