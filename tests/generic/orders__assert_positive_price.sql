{% test orders__assert_positive_price(model, column_name) %}

{{ config(
  enabled= env_var('RUN_THIS_TEST', False)
) }}


with validation as (

    select
        {{ column_name }} as column_name

    from {{ model }}

),

validation_errors as (

    select
        column_name

    from validation
    -- if this is true, then the price field is negative which is bad!
    where column_name < 0

)

select *
from validation_errors

{% endtest %}