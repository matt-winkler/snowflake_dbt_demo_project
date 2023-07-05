
{{
    config(
        materialized="incremental",
        incremental_strategy='merge',
        unique_key='order_key',
        on_schema_change="sync_all_columns"
    )
}}

{% set columns = dynamic_select_columns(node=ref("fct_orders")) %}

select
    {% for col in columns %}
        {{ col }}{%- if not loop.last -%}, {%- endif -%}
    {% endfor %}
from {{ ref("fct_orders") }}
