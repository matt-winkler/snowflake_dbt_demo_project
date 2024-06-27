{%- macro create_constraints(results) -%}
  {%- for res in results -%}
    {%- if res.node.config.materialized in ['table', 'incremental', 'snapshot', 'seed'] -%}
      {%- set table_identifier = res.node.database ~ '.' ~ res.node.schema ~ '.' ~ res.node.name -%}
      {%- do log('on node: ' ~ table_identifier, info=true) -%}
        {%- set columns = res.node.columns -%}
          {%- for col in columns -%}
            {%- do log('on column: ' ~ col, info=true) -%}
            {%- set constraints = columns[col].meta.constraints -%}
            {%- do log('column constraints: ' ~ constraints, info=true) -%}
            {%- do create_unique_key(table_identifier, col) -%}
          {%- endfor %}
    {%- endif -%}
  {%- endfor -%}
{%- endmacro -%}