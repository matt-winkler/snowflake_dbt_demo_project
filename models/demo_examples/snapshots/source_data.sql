
-- original data
select 1 as id, 'foo' as col1, '2022-01-01 00:00:00'::timestamp as updated_at union all
select 2 as id, 'bar' as col1, '2022-01-01 00:00:00'::timestamp as updated_at
union all
select 3 as id, 'baz' as col1, '2022-01-01 00:00:00'::timestamp as updated_at

-- uncomment to update the record with id = 1 for snapshot illustration
--union all select 1 as id, 'qux' as col1, '2022-01-01 01:00:00'::timestamp as updated_at 
