{{
    config(
        materialized='table',
        database='MATT_W_TEST_DB',
        schema='MATT_W_TEST_SCHEMA'
    )
}}

select 1 as id