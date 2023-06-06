
-- how is this NOT a select * ??
-- why query the information schema vs. the source table directly?
-- assuming the target table is long lived (i.e. truncate and reload) can we use an incremental model and sync the columns
{#
    dynamic_select_columns(
        database_name='analytics',
        schema_name='dbt_mwinkler',
        table_name='fct_orders'
    )
#}

{% set columns = 
    dbt_utils.get_column_values(
        table=source('information_schema', 'columns'),
        column='column_name',
        where="table_schema = UPPER('DBT_MWINKLER') and table_name = 'FCT_ORDERS'",
        order_by='ORDINAL_POSITION'
    )
%}

select {% for col in columns %}
         {{col}}
       {% endfor %}
from {{ref('fct_orders')}} -- this could be a source() too - doesn't matter for illustration