{%- macro create_constraints(results) -%}
  {%- for res in results -%}
    {%- if res.node.config.materialized in ['table', 'incremental', 'snapshot', 'seed'] -%}
      {%- set table_identifier = res.node.database ~ '.' ~ res.node.schema ~ '.' ~ res.node.name -%}
      {%- do log('on node: ' ~ table_identifier, info=true) -%}
        {%- set columns = res.node.columns -%}
          {%- for col in columns -%}
            {%- do log('on column: ' ~ col, info=true) -%}
            {%- set constraints = columns[col].meta.constraints -%}
            {%- for const in constraints -%}
              {%- do create_unique_key(table_identifier, column_names=[col]) -%}
            {%- endfor -%}
          {%- endfor %}
    {%- endif -%}
  {%- endfor -%}
{%- endmacro -%}

{%- macro create_constraint_by_type(table_identifier, constraint_type, column_names, fk_column_names=none) -%}
{%- endmacro -%}