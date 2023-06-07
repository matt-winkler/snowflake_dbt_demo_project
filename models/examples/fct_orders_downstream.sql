{#
  NOTES:
    - can modify the select logic in additional macros to identify fact_ vs. dim_ use cases and filter / DISTINCT as needed
    - using an incremental model to make the target table long-lived
    - on_schema_change='sync_all_columns' has the effect of modifying the target table based on the columns present in the source
    - pre_hook leveraged to truncate the target table (if exists) or run a dummy select if it doesn't and needs to be recreated
    - dynamic_select_columns macro identifies the columns from the source table in order based on the information_schema
#}
{{
    config(
        materialized="incremental",
        on_schema_change="sync_all_columns",
        pre_hook="{% if is_incremental() %} truncate table {{this}} {% else %} select 1 as id {% endif %};",
    )
}}

{% set columns = dynamic_select_columns(node=ref("fct_orders")) %}

select
    {% for col in columns %}
        {{ col }}{%- if not loop.last -%}, {%- endif -%}
    {% endfor %}
from {{ ref("fct_orders") }}  -- this could be a source() too - doesn't matter for illustration
