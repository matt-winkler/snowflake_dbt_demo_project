{{
    config(
        materialized='incremental',
        unique_key=['year', 'total_revenue']
    )
}}

select
        year,
        total_revenue
from {{ ref('fct_revenue_summary') }}

{% if is_incremental() %}
where true
{% endif %}