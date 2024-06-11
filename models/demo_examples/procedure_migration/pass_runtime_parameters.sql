{# 
  runtime variables can be passed from the command line e.g. dbt run -s pass_runtime_parameters --vars 'my_custom_date_variable: "2025-01-01"'
  There are also built in dbt variables such as {{run_started_at}} which can be accessed from the jinja context
  Variables can also be set in the dbt_project.yml file
#}

select '{{var('my_custom_date_variable', '2024-01-01')}}' as custom_run_date, '{{run_started_at}}' as default_run_date