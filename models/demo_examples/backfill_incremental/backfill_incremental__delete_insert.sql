{#
  
  This is an example usage of incremental predicates to maximize efficiency of incremental runs.
  
  The idea is to limit the amount of data scanned on *both* the source and target tables
  -- `incremental_predicate` in the model config limits the scan on the target table
  -- filter in the {% if is_incremental() %} block limits the amount of data scanned on the source
  -- also implements cluster_by which performs an explicit sort as a hint to the query optimizer
  -- the `unique_key` can also be removed from the config, which has the effect of overwriting the 
     entire date range.
#}

{{
    config(
        materialized='incremental',
        unique_key='order_key',
        incremental_strategy='delete+insert',
        incremental_predicates=["order_date >= " ~ "'" ~ var('demo__backfill_incremental__start_date') ~ "'", 
                                "order_date <= " ~ "'" ~ var('demo__backfill_incremental__end_date') ~ "'"
                                ],
        cluster_by='order_date'
    )
}}

select * 
from {{ ref('fct_orders') }}
{% if is_incremental() %}
where order_date >= '{{var("demo__backfill_incremental__start_date")}}' AND order_date <= '{{var("demo__backfill_incremental__end_date")}}'
{% endif %}
