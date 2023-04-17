{% snapshot source_data__snapshot %}

    {{
        config(
            unique_key='id',
            strategy='timestamp',
            updated_at='updated_at',
            target_schema=generate_schema_name('tpch_snapshots')
        )
    }}

    select * from {{ ref('source_data') }}

 {% endsnapshot %}