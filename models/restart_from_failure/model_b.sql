select * from {{ref('model_a')}}
union all
-- create a duplicate row
select * from {{ref('model_a')}}