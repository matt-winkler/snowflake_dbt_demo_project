{% snapshot source_data__snapshot %}

    {{
        config(
            unique_key='id',
            strategy='timestamp',
            updated_at='updated_at',
            enabled=False,
            target_schema=target.schema,
            target_database=target.database,
        )
    }}

    select * from {{ ref('source_data') }}

 {% endsnapshot %}