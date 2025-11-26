{{
    config(
        materialized='incremental',
        table_format='iceberg',
        catalog='SNOWFLAKE',
        incremental_strategy='merge',
        unique_key='id',
        on_schema_change='sync_all_columns'
    )
}}


select 1 as id
       ,'example_1' as foo
       ,'example_2'::varchar(134217728) as bar
       --,'example_3'::varchar(134217728) as baz