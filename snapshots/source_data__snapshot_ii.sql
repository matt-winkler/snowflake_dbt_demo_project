/*
            unique_key='id',
            strategy='timestamp',
            updated_at='updated_at',
            target_schema=target.schema,
            target_database=target.database,
            sql_header="ALTER SESSION SET ERROR_ON_NONDETERMINISTIC_MERGE=false;"
*/


{% snapshot source_data__snapshot_ii %}

    {{
        config(
            unique_key='id',
            strategy='timestamp',
            updated_at='updated_at',
            target_schema=target.schema,
            target_database=target.database,
            sql_header="ALTER SESSION SET ERROR_ON_NONDETERMINISTIC_MERGE=false;",
            post_hook="{{mark_end_dates__timestamp(model)}}"
        )
    }}

{% set snapshot_exists = check_snapshot_exists(model) %}

with upstream_data as (
    -- run 1
    
    select 1 as id, 'foo' as col1, '2023-01-01 00:00:00' as updated_at union all
    select 2 as id, 'bar' as col1, '2023-01-01 00:00:00' as updated_at union all
    select 3 as id, 'baz' as col1, '2023-01-01 00:00:00' as updated_at union all
    

    -- run 2

    select 1 as id, 'qaa' as col1, '2023-01-02 00:00:00' as updated_at union all 
    select 1 as id, 'qux' as col1, '2023-01-03 00:00:00' as updated_at

    -- run 3
    --select 1 as id, 'qaz' as col1, '2023-01-04 00:00:00' as updated_at
)

select source.*
from upstream_data source


 {% endsnapshot %}