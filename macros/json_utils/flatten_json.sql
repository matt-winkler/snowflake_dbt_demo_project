{% macro generate_flatten_json(model_name, json_column) %}

{% set get_json_path %}

{# /* get json keys and paths with the FLATTEN function supported by Snowflake */ #}
with low_level_flatten as (
	select f.key as json_key, f.path as json_path, 
	f.value as json_value
	from {{ model_name }}, 
	lateral flatten(input => {{ json_column }}, recursive => true ) f
)

	{# /* get the unique and flattest paths  */ #}
	{# /* you could modify the function to determine the level of nested JSON  */ #}
	select distinct json_path
	from low_level_flatten
	where not contains(json_value, '{')

{% endset %}

{# /* the value in the column will be the attributes of you exploded result  */ #}
{% set res = run_query(get_json_path) %}
{% if execute %}
    {% set res_list = res.columns[0].values() %}
{% else %}
    {% set res_list = [] %}
{% endif %}

{# /* explode JSON columns and format the column names  */ #}
select 
{% for json_path in res_list %}
    {{ json_column }}:{{ json_path }} as {{ json_path | replace(".", "_") | replace("[", "_") | replace("]", "") | replace("'", "") }}{{ ", " if not loop.last else "" }}
{% endfor %}

from {{ model_name }}

{% endmacro %}