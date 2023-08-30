with parsed as (

    {{ json_to_model_sql('source_json', 'data') }}

),

experiments as (
    select
        parsed.id,
        parsed.type,
        flattened_data.value as experiment_id
    from parsed,
        table(flatten(input => experiment_ids)) as flattened_data
),

pages as (
    select
        parsed.id,
        parsed.type,
        flattened_data.value:id as mapped_id,
        flattened_data.value:page as mapped_page
    from parsed,
        table(flatten(input => experiment_mappings)) as flattened_data
),

final as (
    select
        experiments.*,
        pages.mapped_page as experiment_page
    from experiments
    left join pages
        on experiments.id = pages.id
            and experiments.experiment_id = pages.mapped_id
)

select * from final
