
{{
  config(
    materialized = 'incremental',
    unique_key = 'id',
    on_schema_change = 'fail',
    post_hook = set_foreign_key_constraint(
        this_table=this, 
        this_column_name='id', 
        fk_table=target.database ~ '.' ~ target.schema ~ '.' ~ 'test_constraints_fk_table',
        fk_column='id', 
        rely=false
        )
    )
}}

select 1 as id, 'foo' as col1 union all
select 2 as id, 'bar' as col1 union all 
select 3 as id, 'baz' as col1