select * from {{ref('source_data')}}

-- select all records in the snapshot
select * from {{ref('source_data__snapshot')}}

-- select only current records
select * from {{ref('source_data__snapshot')}} where dbt_valid_to IS NULL



