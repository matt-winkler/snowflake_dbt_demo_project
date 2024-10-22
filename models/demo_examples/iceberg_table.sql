{{
    config(
        materialized='table',
        table_format='iceberg',
        external_volume='test',
        base_location_subpath='{{target.alias}}'
    )
}}

select 1 as id