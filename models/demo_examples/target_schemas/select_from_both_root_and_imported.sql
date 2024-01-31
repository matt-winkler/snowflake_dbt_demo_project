with root_project_model as (
    select * from {{ref('fct_order_items')}}
),

package_import_model as (
    select * from {{ref('my_first_dbt_model_src')}}
)

select 1 as id
