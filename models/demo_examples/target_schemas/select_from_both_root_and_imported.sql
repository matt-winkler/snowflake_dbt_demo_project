with root_project_model as (
    select * from {{ref('select_from_root_package')}}
),

package_import_model as (
    select * from {{ref('my_first_dbt_model_src')}}
)

select 1 as id
