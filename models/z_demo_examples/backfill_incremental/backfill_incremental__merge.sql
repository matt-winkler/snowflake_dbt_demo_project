{#
  
  This is an example usage of incremental predicates to maximize efficiency of incremental runs.
  
  The idea is to limit the amount of data scanned on *both* the source and target tables
  -- `incremental_predicate` in the model config limits the scan on the target table
  -- filter in the {% if is_incremental() %} block limits the amount of data scanned on the source
  -- also implements cluster_by which performs an explicit sort as a hint to the query optimizer
#}

{{
    config(
        materialized='incremental',
        unique_key='order_key',
        incremental_strategy='merge',
        incremental_predicates=["DBT_INTERNAL_DEST.order_date >= " ~ "'" ~ var('demo__backfill_incremental__start_date') ~ "'", 
                                "DBT_INTERNAL_DEST.order_date <= " ~ "'" ~ var('demo__backfill_incremental__end_date') ~ "'"
                                ],
        cluster_by='order_date',
        on_schema_change='sync_all_columns'
    )
}}

select * 
from {{ ref('fct_orders') }}
{% if is_incremental() %}
where order_date >= '{{var("demo__backfill_incremental__start_date")}}' AND order_date <= '{{var("demo__backfill_incremental__end_date")}}'
{% endif %}
