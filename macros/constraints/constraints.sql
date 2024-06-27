{%- macro create_constraints(results) -%}
  {%- for res in results -%}
    {%- if res.node.config.materialized in ['table', 'incremental'] -%}
      {%- do log('on node: ' ~ res.node.name, info=true) -%}
        {%- set columns = res.node.columns -%}
          {%- for col in columns -%}
            {%- do log('on column: ' ~ col, info=true) -%}
            {%- set constraints = columns[col].meta.constraints -%}
            {%- do log('column constraints: ' ~ constraints, info=true) -%}
          {%- endfor %}
    {%- endif -%}
  {%- endfor -%}
{%- endmacro -%}