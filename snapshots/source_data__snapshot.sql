{% snapshot source_data__snapshot %}

    {{
        config(
            unique_key='id',
            strategy='check',
            check_cols=['region'],
            updated_at='updated_at',
            target_schema=target.schema,
            target_database=target.database,
        )
    }}

    select 1 as id, 'west' as region, '2024-01-01 00:00:00' as updated_at union all
    select 2 as id, 'central' as region, '2024-01-01 00:00:00' as updated_at 
    --union all select 1 as id, 'midwest' as region, '2024-01-02 00:00:00' as updated_at

 {% endsnapshot %}