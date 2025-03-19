{{
    config(
        severity='error',
        tags = ['finance']
    )
}}
-- pr trigger

{{ test_all_values_gte_zero('stg_tpch_orders', 'total_price') }}