
{{
    config(
        materialized="incremental",
        incremental_strategy='merge',
        unique_key='order_key',
        on_schema_change="fail",
        snowflake_warehouse=get_incremental_model_warehouse()
    )
}}

{% set columns = dynamic_select_columns(node=ref("fct_orders", v='0')) %}

select
    {% for col in columns %}
        {{ col }}{%- if not loop.last -%}, {%- endif -%}
    {% endfor %}
from {{ ref("fct_orders", v='0') }}
