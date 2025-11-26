{{
    config(
        materialized='table',
        table_format='iceberg',
        catalog='SNOWFLAKE'
    )
}}

select 1 as id