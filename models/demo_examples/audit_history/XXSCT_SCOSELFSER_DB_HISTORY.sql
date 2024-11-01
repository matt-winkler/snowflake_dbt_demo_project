{{ 
    config(
        materialized = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key = ['query_id', 'created_at']
    )
}}

-- does the view used here have a filter to pull a single date?
select 1 as id