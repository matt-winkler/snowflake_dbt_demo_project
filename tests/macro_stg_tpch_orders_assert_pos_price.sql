{{
    config(
        enabled=true,
        severity='error',
        tags = ['ci_only']
    )
}}


{{ test_all_values_gte_zero('stg_tpch_orders', 'total_price') }}