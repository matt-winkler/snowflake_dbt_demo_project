select * from {{ref('model_b')}}
union all 
select * from {{ref('model_c')}}