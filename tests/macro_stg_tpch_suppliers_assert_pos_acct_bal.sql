{{
    config(
        enabled=true,
        schema='analytics',
        database='analytics_mwinkler_dbt_workspace',
        tags = ['finance']
    )
}}


{{ test_all_values_gte_zero('stg_tpch_suppliers', 'account_balance') }}