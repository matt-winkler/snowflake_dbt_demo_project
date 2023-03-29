{% snapshot source_data__snapshot %}

    {{
        config(
            unique_key='id',
            strategy='timestamp',
            updated_at='updated_at',
            alias='tst',
        )
    }}

    select * from {{ ref('source_data') }}

 {% endsnapshot %}