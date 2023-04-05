{#
  
  This is an example usage of incremental predicates to maximize efficiency of incremental runs.
  
  The idea is to limit the amount of data scanned on *both* the source and target tables
  -- `incremental_predicate` in the model config limits the scan on the target table
  -- filter in the {% if is_incremental() %} block limits the amount of data scanned on the source
#}

{{
    config(
        materialized='incremental',
        unique_key='order_date',
        incremental_strategy='delete+insert',
        incremental_predicates=["order_date >= " ~ "'" ~ var('demo__backfill_incremental__start_date') ~ "'", 
                                "order_date <= " ~ "'" ~ var('demo__backfill_incremental__end_date') ~ "'"
                                ]
    )
}}

select * 
from {{ ref('fct_orders') }}
{% if is_incremental() %}
where order_date BETWEEN '{{var("demo__backfill_incremental__start_date")}}' AND '{{var("demo__backfill_incremental__end_date")}}'
{% endif %}
